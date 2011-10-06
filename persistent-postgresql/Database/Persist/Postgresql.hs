{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
-- | A postgresql backend for persistent.
module Database.Persist.Postgresql
    ( withPostgresqlPool
    , withPostgresqlConn
    , module Database.Persist
    , module Database.Persist.GenericSql
    , PostgresConf (..)
    , P.ConnectInfo (..)
    , P.defaultConnectInfo
    ) where

import Database.Persist hiding (Update)
import Database.Persist.Base hiding (Add, Update)
import Database.Persist.GenericSql hiding (Key(..))
import Database.Persist.GenericSql.Internal

import Data.List (intercalate)
import Data.IORef
import qualified Data.Map as Map
import Data.Either (partitionEithers)
import Control.Arrow
import Data.List (sort, groupBy)
import Data.Function (on)
import Control.Monad.IO.Control (MonadControlIO)
import Control.Monad.IO.Class (liftIO)

import Data.ByteString (ByteString)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import Data.Text (Text, pack)
import Data.Object (fromMapping, lookupScalar, ObjectExtractError)
import Data.Neither (meither, MEither (..))
import Data.String (fromString)

import qualified Database.PostgreSQL.Simple as P
import Database.PostgreSQL.Simple.Param (Param (..))
import Database.PostgreSQL.Simple.QueryResults (QueryResults (..))
import Database.PostgreSQL.Simple.Result (Result (convert))
import Database.PostgreSQL.Simple.Types (Null (Null))

withPostgresqlPool :: MonadControlIO m
                   => P.ConnectInfo
                   -> Int -- ^ number of connections to open
                   -> (ConnectionPool -> m a) -> m a
withPostgresqlPool s = withSqlPool $ open' s

withPostgresqlConn :: MonadControlIO m => P.ConnectInfo -> (Connection -> m a) -> m a
withPostgresqlConn = withSqlConn . open'

open' :: P.ConnectInfo -> IO Connection
open' s = do
    conn <- P.connect s
    smap <- newIORef Map.empty
    return Connection
        { prepare = prepare' conn
        , stmtMap = smap
        , insertSql = insertSql'
        , close = P.close conn
        , migrateSql = migrate'
        , begin = const $ return ()
        , commitC = const $ P.commit conn
        , rollbackC = const $ P.rollback conn
        , escapeName = escape
        , noLimit = "LIMIT ALL"
        }

prepare' :: P.Connection -> Text -> IO Statement
prepare' conn sql = do
    let query = fromString $ T.unpack sql
    return Statement
        { finalize = return ()
        , reset = return ()
        , execute = execute' conn query
        , withStmt = withStmt' conn query
        }

insertSql' :: RawName -> [RawName] -> Either Text (Text, Text)
insertSql' t cols = Left $ pack $ concat
    [ "INSERT INTO "
    , escape t
    , "("
    , intercalate "," $ map escape cols
    , ") VALUES("
    , intercalate "," (map (const "?") cols)
    , ") RETURNING id"
    ]

execute' :: P.Connection -> P.Query -> [PersistValue] -> IO ()
execute' conn query vals = do
    _ <- P.execute conn query vals
    return ()

withStmt' :: MonadControlIO m
          => P.Connection
          -> P.Query
          -> [PersistValue]
          -> (RowPopper m -> m a)
          -> m a
withStmt' conn query vals f = do
    iresult <- liftIO $ P.query conn query vals >>= newIORef
    f $ pop iresult
  where
    pop ir = liftIO $ atomicModifyIORef ir $ \res ->
        case res of
            [] -> ([], Nothing)
            (r:rs) -> (rs, Just r)

instance Param PersistValue where
    render (PersistText t) = render t
    render (PersistByteString bs) = render bs
    render (PersistInt64 i) = render i
    render (PersistDouble d) = render d
    render (PersistBool b) = render b
    render (PersistDay d) = render d
    render (PersistTimeOfDay t) = render t
    render (PersistUTCTime t) = render t
    render PersistNull = render Null
    render PersistList{} = error "Refusing to serialize a PersistList to a PostgreSQL value"
    render PersistMap{} = error "Refusing to serialize a PersistMap to a PostgreSQL value"
    render PersistObjectId{}  = error "Refusing to serialize a PersistObjectId to a PostgreSQL value"

instance QueryResults [PersistValue] where
    convertResults = zipWith convert

instance Result PersistValue where
    convert _ Nothing = PersistNull
    convert _ (Just bs) = PersistByteString bs -- FIXME

migrate' :: PersistEntity val
         => (Text -> IO Statement)
         -> val
         -> IO (Either [Text] [(Bool, Text)])
migrate' getter val = do
    let name = rawTableName $ entityDef val
    old <- getColumns getter name
    case partitionEithers old of
        ([], old'') -> do
            let old' = partitionEithers old''
            let new = mkColumns val
            if null old
                then do
                    let addTable = AddTable $ concat
                            [ "CREATE TABLE "
                            , escape name
                            , "(id SERIAL PRIMARY KEY UNIQUE"
                            , concatMap (\x -> ',' : showColumn x) $ fst new
                            , ")"
                            ]
                    let rest = flip concatMap (snd new) $ \(uname, ucols) ->
                            [AlterTable name $ AddUniqueConstraint uname ucols]
                    return $ Right $ map showAlterDb $ addTable : rest
                else do
                    let (acs, ats) = getAlters new old'
                    let acs' = map (AlterColumn name) acs
                    let ats' = map (AlterTable name) ats
                    return $ Right $ map showAlterDb $ acs' ++ ats'
        (errs, _) -> return $ Left errs

data AlterColumn = Type SqlType | IsNull | NotNull | Add Column | Drop
                 | Default String | NoDefault | Update String
                 | AddReference RawName | DropReference RawName
type AlterColumn' = (RawName, AlterColumn)

data AlterTable = AddUniqueConstraint RawName [RawName]
                | DropConstraint RawName

data AlterDB = AddTable String
             | AlterColumn RawName AlterColumn'
             | AlterTable RawName AlterTable

-- | Returns all of the columns in the given table currently in the database.
getColumns :: (Text -> IO Statement)
           -> RawName -> IO [Either Text (Either Column UniqueDef')]
getColumns getter name = do
    stmt <- getter "SELECT column_name,is_nullable,udt_name,column_default FROM information_schema.columns WHERE table_name=? AND column_name <> 'id'"
    cs <- withStmt stmt [PersistText $ pack $ unRawName name] helper
    stmt' <- getter
        "SELECT constraint_name, column_name FROM information_schema.constraint_column_usage WHERE table_name=? AND column_name <> 'id' ORDER BY constraint_name, column_name"
    us <- withStmt stmt' [PersistText $ pack $ unRawName name] helperU
    return $ cs ++ us
  where
    getAll pop front = do
        x <- pop
        case x of
            Nothing -> return $ front []
            Just [PersistByteString con, PersistByteString col] ->
                getAll pop (front . (:) (bsToChars con, bsToChars col))
            Just _ -> getAll pop front -- FIXME error message?
    helperU pop = do
        rows <- getAll pop id
        return $ map (Right . Right . (RawName . fst . head &&& map (RawName . snd)))
               $ groupBy ((==) `on` fst) rows
    helper pop = do
        x <- pop
        case x of
            Nothing -> return []
            Just x' -> do
                col <- getColumn getter name x'
                let col' = case col of
                            Left e -> Left e
                            Right c -> Right $ Left c
                cols <- helper pop
                return $ col' : cols

getAlters :: ([Column], [UniqueDef'])
          -> ([Column], [UniqueDef'])
          -> ([AlterColumn'], [AlterTable])
getAlters (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    getAltersC [] old = map (\x -> (cName x, Drop)) old
    getAltersC (new:news) old =
        let (alters, old') = findAlters new old
         in alters ++ getAltersC news old'
    getAltersU [] old = map (DropConstraint . fst) old
    getAltersU ((name, cols):news) old =
        case lookup name old of
            Nothing -> AddUniqueConstraint name cols : getAltersU news old
            Just ocols ->
                let old' = filter (\(x, _) -> x /= name) old
                 in if sort cols == ocols
                        then getAltersU news old'
                        else  DropConstraint name
                            : AddUniqueConstraint name cols
                            : getAltersU news old'

getColumn :: (Text -> IO Statement)
          -> RawName -> [PersistValue]
          -> IO (Either Text Column)
getColumn getter tname
        [PersistByteString x, PersistByteString y,
         PersistByteString z, d] =
    case d' of
        Left s -> return $ Left s
        Right d'' ->
            case getType $ bsToChars z of
                Left s -> return $ Left s
                Right t -> do
                    let cname = RawName $ bsToChars x
                    ref <- getRef cname
                    return $ Right $ Column cname (bsToChars y == "YES")
                                     t d'' ref
  where
    getRef cname = do
        let sql = pack $ concat
                [ "SELECT COUNT(*) FROM "
                , "information_schema.table_constraints "
                , "WHERE table_name=? "
                , "AND constraint_type='FOREIGN KEY' "
                , "AND constraint_name=?"
                ]
        let ref = refName tname cname
        stmt <- getter sql
        withStmt stmt
                     [ PersistText $ pack $ unRawName tname
                     , PersistText $ pack $ unRawName ref
                     ] $ \pop -> do
            Just [PersistInt64 i] <- pop
            return $ if i == 0 then Nothing else Just (RawName "", ref)
    d' = case d of
            PersistNull -> Right Nothing
            PersistByteString a -> Right $ Just $ bsToChars a
            _ -> Left $ pack $ "Invalid default column: " ++ show d
    getType "int4" = Right $ SqlInt32
    getType "int8" = Right $ SqlInteger
    getType "varchar" = Right $ SqlString
    getType "date" = Right $ SqlDay
    getType "bool" = Right $ SqlBool
    getType "timestamp" = Right $ SqlDayTime
    getType "float4" = Right $ SqlReal
    getType "float8" = Right $ SqlReal
    getType "bytea" = Right $ SqlBlob
    getType a = Left $ pack $ "Unknown type: " ++ a
getColumn _ _ x =
    return $ Left $ pack $ "Invalid result from information_schema: " ++ show x

findAlters :: Column -> [Column] -> ([AlterColumn'], [Column])
findAlters col@(Column name isNull type_ def ref) cols =
    case filter (\c -> cName c == name) cols of
        [] -> ([(name, Add col)], cols)
        Column _ isNull' type_' def' ref':_ ->
            let refDrop Nothing = []
                refDrop (Just (_, cname)) = [(name, DropReference cname)]
                refAdd Nothing = []
                refAdd (Just (tname, _)) = [(name, AddReference tname)]
                modRef =
                    if fmap snd ref == fmap snd ref'
                        then []
                        else refDrop ref' ++ refAdd ref
                modNull = case (isNull, isNull') of
                            (True, False) -> [(name, IsNull)]
                            (False, True) ->
                                let up = case def of
                                            Nothing -> id
                                            Just s -> (:) (name, Update s)
                                 in up [(name, NotNull)]
                            _ -> []
                modType = if type_ == type_' then [] else [(name, Type type_)]
                modDef =
                    if def == def'
                        then []
                        else case def of
                                Nothing -> [(name, NoDefault)]
                                Just s -> [(name, Default s)]
             in (modRef ++ modDef ++ modNull ++ modType,
                 filter (\c -> cName c /= name) cols)

showColumn :: Column -> String
showColumn (Column n nu t def ref) = concat
    [ escape n
    , " "
    , showSqlType t
    , " "
    , if nu then "NULL" else "NOT NULL"
    , case def of
        Nothing -> ""
        Just s -> " DEFAULT " ++ s
    , case ref of
        Nothing -> ""
        Just (s, _) -> " REFERENCES " ++ escape s
    ]

showSqlType :: SqlType -> String
showSqlType SqlString = "VARCHAR"
showSqlType SqlInt32 = "INT4"
showSqlType SqlInteger = "INT8"
showSqlType SqlReal = "DOUBLE PRECISION"
showSqlType SqlDay = "DATE"
showSqlType SqlTime = "TIME"
showSqlType SqlDayTime = "TIMESTAMP"
showSqlType SqlBlob = "BYTEA"
showSqlType SqlBool = "BOOLEAN"

showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AlterColumn t (c, ac)) =
    (isUnsafe ac, pack $ showAlter t (c, ac))
  where
    isUnsafe Drop = True
    isUnsafe _ = False
showAlterDb (AlterTable t at) = (False, pack $ showAlterTable t at)

showAlterTable :: RawName -> AlterTable -> String
showAlterTable table (AddUniqueConstraint cname cols) = concat
    [ "ALTER TABLE "
    , escape table
    , " ADD CONSTRAINT "
    , escape cname
    , " UNIQUE("
    , intercalate "," $ map escape cols
    , ")"
    ]
showAlterTable table (DropConstraint cname) = concat
    [ "ALTER TABLE "
    , escape table
    , " DROP CONSTRAINT "
    , escape cname
    ]

showAlter :: RawName -> AlterColumn' -> String
showAlter table (n, Type t) =
    concat
        [ "ALTER TABLE "
        , escape table
        , " ALTER COLUMN "
        , escape n
        , " TYPE "
        , showSqlType t
        ]
showAlter table (n, IsNull) =
    concat
        [ "ALTER TABLE "
        , escape table
        , " ALTER COLUMN "
        , escape n
        , " DROP NOT NULL"
        ]
showAlter table (n, NotNull) =
    concat
        [ "ALTER TABLE "
        , escape table
        , " ALTER COLUMN "
        , escape n
        , " SET NOT NULL"
        ]
showAlter table (_, Add col) =
    concat
        [ "ALTER TABLE "
        , escape table
        , " ADD COLUMN "
        , showColumn col
        ]
showAlter table (n, Drop) =
    concat
        [ "ALTER TABLE "
        , escape table
        , " DROP COLUMN "
        , escape n
        ]
showAlter table (n, Default s) =
    concat
        [ "ALTER TABLE "
        , escape table
        , " ALTER COLUMN "
        , escape n
        , " SET DEFAULT "
        , s
        ]
showAlter table (n, NoDefault) = concat
    [ "ALTER TABLE "
    , escape table
    , " ALTER COLUMN "
    , escape n
    , " DROP DEFAULT"
    ]
showAlter table (n, Update s) = concat
    [ "UPDATE "
    , escape table
    , " SET "
    , escape n
    , "="
    , s
    , " WHERE "
    , escape n
    , " IS NULL"
    ]
showAlter table (n, AddReference t2) = concat
    [ "ALTER TABLE "
    , escape table
    , " ADD CONSTRAINT "
    , escape $ refName table n
    , " FOREIGN KEY("
    , escape n
    , ") REFERENCES "
    , escape t2
    ]
showAlter table (_, DropReference cname) =
    "ALTER TABLE " ++ escape table ++ " DROP CONSTRAINT " ++ escape cname

escape :: RawName -> String
escape (RawName s) =
    '"' : go s ++ "\""
  where
    go "" = ""
    go ('"':xs) = "\"\"" ++ go xs
    go (x:xs) = x : go xs

bsToChars :: ByteString -> String
bsToChars = T.unpack . T.decodeUtf8With T.lenientDecode

-- | Information required to connect to a postgres database
data PostgresConf = PostgresConf
    { pgConnStr  :: P.ConnectInfo
    , pgPoolSize :: Int
    }

instance PersistConfig PostgresConf where
    type PersistConfigBackend PostgresConf = SqlPersist
    type PersistConfigPool PostgresConf = ConnectionPool
    withPool (PostgresConf cs size) = withPostgresqlPool cs size
    runPool _ = runSqlPool
    loadConfig e' = meither Left Right $ do
        e <- go $ fromMapping e'
        _db <- go $ lookupScalar "database" e
        pool' <- go $ lookupScalar "poolsize" e
        _pool <- safeRead "poolsize" pool'

        error "FIXME" {-
        -- TODO: default host/port?
        connparts <- forM ["user", "password", "host", "port"] $ \k -> do
            v <- go $ lookupScalar k e
            return $ T.concat [k, "=", v, " "]

        let conn = T.concat connparts

        return $ PostgresConf (T.concat [conn, " dbname=", db]) pool
        -}
      where
        go :: MEither ObjectExtractError a -> MEither String a
        go (MLeft e) = MLeft $ show e
        go (MRight a) = MRight a

safeRead :: String -> T.Text -> MEither String Int
safeRead name t = case reads s of
    (i, _):_ -> MRight i
    []       -> MLeft $ concat ["Invalid value for ", name, ": ", s]
  where
    s = T.unpack t
