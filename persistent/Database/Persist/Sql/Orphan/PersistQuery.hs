{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Sql.Orphan.PersistQuery
    ( deleteWhereCount
    , updateWhereCount
    , decorateSQLWithLimitOffset
    ) where

import Database.Persist
import Database.Persist.Class.PersistQuery
import Database.Persist.Sql.Types
import Database.Persist.Sql.Class
import Database.Persist.Sql.Raw
import Database.Persist.Sql.Orphan.PersistStore ()
import qualified Data.Text as T
import Data.Text (Text)
import Data.Monoid (Monoid (..), (<>))
import Data.Int (Int64)
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource (with)
import Control.Exception (throwIO)
import qualified Data.Conduit.List as CL
import Data.Conduit
import Data.ByteString.Char8 (readInteger)
import Data.Maybe (isJust)
import Data.List (transpose, inits, find)
import Control.Monad.Reader (runReaderT)

-- orphaned instance for convenience of modularity
instance PersistQueryImpl SqlBackend where
    updateImpl _ [] _ = return ()
    updateImpl k upds conn = do
        let go'' n Assign = n <> "=?"
            go'' n Add = T.concat [n, "=", n, "+?"]
            go'' n Subtract = T.concat [n, "=", n, "-?"]
            go'' n Multiply = T.concat [n, "=", n, "*?"]
            go'' n Divide = T.concat [n, "=", n, "/?"]
        let go' (x, pu) = go'' (connEscapeName conn x) pu
        let wher = case entityPrimary t of
                Just pdef -> T.intercalate " AND " $ map (\fld -> connEscapeName conn (snd fld) <> "=? ") $ primaryFields pdef
                Nothing   -> connEscapeName conn (entityID t) <> "=?"
        let sql = T.concat
                [ "UPDATE "
                , connEscapeName conn $ entityDB t
                , " SET "
                , T.intercalate "," $ map (go' . go) upds
                , " WHERE "
                , wher
                ]
        flip runReaderT conn $ rawExecute sql $
            map updatePersistValue upds `mappend` keyToValues k
      where
        t = entityDef $ dummyFromKey k
        go x = (fieldDB $ updateFieldDef x, updateUpdate x)

    countImpl filts conn = do
        let wher = if null filts
                    then ""
                    else filterClause False conn filts
        let sql = mconcat
                [ "SELECT COUNT(*) FROM "
                , connEscapeName conn $ entityDB t
                , wher
                ]
        with (rawQueryResource sql (getFiltsValues conn filts) conn) $ \src -> src $$ do
            mm <- CL.head
            case mm of
              Just [PersistInt64 i] -> return $ fromIntegral i
              Just [PersistDouble i] ->return $ fromIntegral (truncate i :: Int64) -- gb oracle
              Just [PersistByteString i] -> case readInteger i of -- gb mssql 
                                              Just (ret,"") -> return $ fromIntegral ret
                                              xs -> error $ "invalid number i["++show i++"] xs[" ++ show xs ++ "]"
              Just xs -> error $ "count:invalid sql  return xs["++show xs++"] sql["++show sql++"]"
              Nothing -> error $ "count:invalid sql returned nothing sql["++show sql++"]"
      where
        t = entityDef $ dummyFromFilts filts

    selectSourceImpl filts opts conn =
        ($= CL.mapM parse) `fmap` rawQueryResource  sql (getFiltsValues conn filts) conn
      where
        composite = isJust $ entityPrimary t
        (limit, offset, orders) = limitOffsetOrder opts

        parse vals =
          case entityPrimary t of
            Just pdef -> 
                  let pks = map fst $ primaryFields pdef
                      keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) vals
                  in case fromPersistValuesComposite' keyvals vals of
                      Left s -> liftIO $ throwIO $ PersistMarshalError s
                      Right row -> return row
            Nothing -> 
                  case fromPersistValues' vals of
                    Left s -> liftIO $ throwIO $ PersistMarshalError s
                    Right row -> return row

        t = entityDef $ dummyFromFilts filts
        fromPersistValues' (k:xs) =
            case fromPersistValues xs of
                Left e -> Left e
                Right xs' ->
                    case keyFromValues [k] of
                        Nothing -> error $ "fromPersistValues': keyFromValues failed on " ++ show k
                        Just k' -> Right (Entity k' xs')
        fromPersistValues' xs = Left $ T.pack ("error in fromPersistValues' xs=" ++ show xs)

        fromPersistValuesComposite' keyvals xs =
            case fromPersistValues xs of
                Left e -> Left e
                Right xs' ->
                    case keyFromValues keyvals of
                        Nothing -> error "fromPersistValuesComposite': keyFromValues failed"
                        Just key -> Right (Entity key xs')

        wher = if null filts
                    then ""
                    else filterClause False conn filts
        ord =
            case map (orderClause False conn) orders of
                [] -> ""
                ords -> " ORDER BY " <> T.intercalate "," ords
        cols = T.intercalate ","
             $ ((if composite then [] else [connEscapeName conn $ entityID t]) 
             <> map (connEscapeName conn . fieldDB) (entityFields t))
        sql = connLimitOffset conn (limit,offset) (not (null orders)) $ mconcat
            [ "SELECT "
            , cols
            , " FROM "
            , connEscapeName conn $ entityDB t
            , wher
            , ord
            ]

    selectKeysImpl filts opts conn =
        ($= CL.mapM parse) `fmap` rawQueryResource sql (getFiltsValues conn filts) conn
      where
        t = entityDef $ dummyFromFilts filts
        cols = case entityPrimary t of 
                     Just pdef -> T.intercalate "," $ map (connEscapeName conn . snd) $ primaryFields pdef
                     Nothing   -> connEscapeName conn $ entityID t
                      
        wher = if null filts
                    then ""
                    else filterClause False conn filts
        sql = connLimitOffset conn (limit,offset) (not (null orders)) $ mconcat
            [ "SELECT "
            , cols
            , " FROM "
            , connEscapeName conn $ entityDB t
            , wher
            , ord
            ]

        (limit, offset, orders) = limitOffsetOrder opts

        ord =
            case map (orderClause False conn) orders of
                [] -> ""
                ords -> " ORDER BY " <> T.intercalate "," ords

        parse xs = do
            keyvals <- case entityPrimary t of
                      Nothing -> do
                        case xs of
                           [PersistInt64 x] -> return [PersistInt64 x]
                           [PersistDouble x] -> return [PersistInt64 (truncate x)] -- oracle returns Double 
                           _ -> liftIO $ throwIO $ PersistMarshalError $ "Unexpected in selectKeys False: " <> T.pack (show xs)
                      Just pdef -> 
                           let pks = map fst $ primaryFields pdef
                               keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) xs
                           in  return keyvals
            case keyFromValues keyvals of
                Just k -> return k
                Nothing -> error "selectKeysImpl: keyFromValues failed"

    deleteWhereImpl filts conn = do
        _ <- runReaderT (deleteWhereCount filts) conn
        return ()

    updateWhereImpl filts upds conn = do
        _ <- runReaderT (updateWhereCount filts upds) conn
        return ()

-- | Same as 'deleteWhere', but returns the number of rows affected.
--
-- Since 1.1.5
deleteWhereCount :: (PersistEntity SqlBackend val, MonadSqlPersist m)
                 => [Filter val]
                 -> m Int64
deleteWhereCount filts = do
    conn <- askSqlConn
    let t = entityDef $ dummyFromFilts filts
    let wher = if null filts
                then ""
                else filterClause False conn filts
        sql = mconcat
            [ "DELETE FROM "
            , connEscapeName conn $ entityDB t
            , wher
            ]
    rawExecuteCount sql $ getFiltsValues conn filts

-- | Same as 'updateWhere', but returns the number of rows affected.
--
-- Since 1.1.5
updateWhereCount :: (PersistEntity SqlBackend val, MonadSqlPersist m)
                 => [Filter val]
                 -> [Update val]
                 -> m Int64
updateWhereCount _ [] = return 0
updateWhereCount filts upds = do
    conn <- askSqlConn
    let wher = if null filts
                then ""
                else filterClause False conn filts
    let sql = mconcat
            [ "UPDATE "
            , connEscapeName conn $ entityDB t
            , " SET "
            , T.intercalate "," $ map (go' conn . go) upds
            , wher
            ]
    let dat = map updatePersistValue upds `mappend`
              getFiltsValues conn filts
    rawExecuteCount sql dat
  where
    t = entityDef $ dummyFromFilts filts
    go'' n Assign = n <> "=?"
    go'' n Add = mconcat [n, "=", n, "+?"]
    go'' n Subtract = mconcat [n, "=", n, "-?"]
    go'' n Multiply = mconcat [n, "=", n, "*?"]
    go'' n Divide = mconcat [n, "=", n, "/?"]
    go' conn (x, pu) = go'' (connEscapeName conn x) pu
    go x = (fieldDB $ updateFieldDef x, updateUpdate x)

updateFieldDef :: PersistEntity SqlBackend v => Update v -> FieldDef SqlType
updateFieldDef (Update f _ _) = persistFieldDef f

dummyFromFilts :: [Filter v] -> Maybe v
dummyFromFilts _ = Nothing

getFiltsValues :: forall val.  PersistEntity SqlBackend val => SqlBackend -> [Filter val] -> [PersistValue]
getFiltsValues conn = snd . filterClauseHelper False False conn OrNullNo

data OrNull = OrNullYes | OrNullNo

filterClauseHelper :: PersistEntity SqlBackend val
             => Bool -- ^ include table name?
             -> Bool -- ^ include WHERE?
             -> SqlBackend
             -> OrNull
             -> [Filter val]
             -> (Text, [PersistValue])
filterClauseHelper includeTable includeWhere conn orNull filters =
    (if not (T.null sql) && includeWhere
        then " WHERE " <> sql
        else sql, vals)
  where
    (sql, vals) = combineAND filters
    combineAND = combine " AND "

    combine s fs =
        (T.intercalate s $ map wrapP a, mconcat b)
      where
        (a, b) = unzip $ map go fs
        wrapP x = T.concat ["(", x, ")"]

    go (BackendFilter _) = error "BackendFilter not expected"
    go (FilterAnd []) = ("1=1", [])
    go (FilterAnd fs) = combineAND fs
    go (FilterOr []) = ("1=0", [])
    go (FilterOr fs)  = combine " OR " fs
    go (Filter field value pfilter) = 
        let t = entityDef $ dummyFromFilts [Filter field value pfilter]
        in case (fieldDB (persistFieldDef field) == DBName "id", entityPrimary t, allVals) of
            -- need to check the id field in a safer way: entityId? 
                 (True, Just pdef, (PersistList ys:_)) -> 
                    if length (primaryFields pdef) /= length ys 
                       then error $ "wrong number of entries in primaryFields vs PersistList allVals=" ++ show allVals
                    else
                      case (allVals, pfilter, isCompFilter pfilter) of
                        ([PersistList xs], Eq, _) -> 
                           let sqlcl=T.intercalate " and " (map (\a -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "? ")  (primaryFields pdef))
                           in (wrapSql sqlcl,xs)
                        ([PersistList xs], Ne, _) -> 
                           let sqlcl=T.intercalate " or " (map (\a -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "? ")  (primaryFields pdef))
                           in (wrapSql sqlcl,xs)
                        (_, In, _) -> 
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "(" <> T.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (primaryFields pdef) xxs)
                           in (wrapSql (T.intercalate " and " (map wrapSql sqls)), concat xxs)
                        (_, NotIn, _) -> 
                           let xxs = transpose (map fromPersistList allVals)
                               sqls=map (\(a,xs) -> connEscapeName conn (snd a) <> showSqlFilter pfilter <> "(" <> T.intercalate "," (replicate (length xs) " ?") <> ") ") (zip (primaryFields pdef) xxs)
                           in (wrapSql (T.intercalate " or " (map wrapSql sqls)), concat xxs)
                        ([PersistList xs], _, True) -> 
                           let zs = tail (inits (primaryFields pdef))
                               sql1 = map (\b -> wrapSql (T.intercalate " and " (map (\(i,a) -> sql2 (i==length b) a) (zip [1..] b)))) zs
                               sql2 islast a = connEscapeName conn (snd a) <> (if islast then showSqlFilter pfilter else showSqlFilter Eq) <> "? "
                               sqlcl = T.intercalate " or " sql1
                           in (wrapSql sqlcl, concat (tail (inits xs)))
                        (_, BackendSpecificFilter _, _) -> error "unhandled type BackendSpecificFilter for composite/non id primary keys"
                        _ -> error $ "unhandled type/filter for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList="++show allVals
                 (True, Just pdef, _) -> error $ "unhandled error for composite/non id primary keys pfilter=" ++ show pfilter ++ " persistList=" ++ show allVals ++ " pdef=" ++ show pdef

                 _ ->   case (isNull, pfilter, varCount) of
                            (True, Eq, _) -> (name <> " IS NULL", [])
                            (True, Ne, _) -> (name <> " IS NOT NULL", [])
                            (False, Ne, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " <> "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            -- We use 1=2 (and below 1=1) to avoid using TRUE and FALSE, since
                            -- not all databases support those words directly.
                            (_, In, 0) -> ("1=2" <> orNullSuffix, [])
                            (False, In, _) -> (name <> " IN " <> qmarks <> orNullSuffix, allVals)
                            (True, In, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            (_, NotIn, 0) -> ("1=1", [])
                            (False, NotIn, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NULL OR "
                                , name
                                , " NOT IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            (True, NotIn, _) -> (T.concat
                                [ "("
                                , name
                                , " IS NOT NULL AND "
                                , name
                                , " NOT IN "
                                , qmarks
                                , ")"
                                ], notNullVals)
                            _ -> (name <> showSqlFilter pfilter <> "?" <> orNullSuffix, allVals) 

      where
        isCompFilter Lt = True
        isCompFilter Le = True
        isCompFilter Gt = True
        isCompFilter Ge = True
        isCompFilter _ =  False
        
        wrapSql sqlcl = "(" <> sqlcl <> ")"
        fromPersistList (PersistList xs) = xs
        fromPersistList other = error $ "expected PersistList but found " ++ show other
        
        filterValueToPersistValues :: forall a.  PersistField a => Either a [a] -> [PersistValue]
        filterValueToPersistValues v = map toPersistValue $ either return id v

        orNullSuffix =
            case orNull of
                OrNullYes -> mconcat [" OR ", name, " IS NULL"]
                OrNullNo -> ""

        isNull = any (== PersistNull) allVals
        notNullVals = filter (/= PersistNull) allVals
        allVals = filterValueToPersistValues value
        tn = connEscapeName conn $ entityDB
           $ entityDef $ dummyFromFilts [Filter field value pfilter]
        name =
            (if includeTable
                then ((tn <> ".") <>)
                else id)
            $ connEscapeName conn $ fieldDB $ persistFieldDef field
        qmarks = case value of
                    Left _ -> "?"
                    Right x ->
                        let x' = filter (/= PersistNull) $ map toPersistValue x
                         in "(" <> T.intercalate "," (map (const "?") x') <> ")"
        varCount = case value of
                    Left _ -> 1
                    Right x -> length x
        showSqlFilter Eq = "="
        showSqlFilter Ne = "<>"
        showSqlFilter Gt = ">"
        showSqlFilter Lt = "<"
        showSqlFilter Ge = ">="
        showSqlFilter Le = "<="
        showSqlFilter In = " IN "
        showSqlFilter NotIn = " NOT IN "
        showSqlFilter (BackendSpecificFilter s) = s

updatePersistValue :: Update v -> PersistValue
updatePersistValue (Update _ v _) = toPersistValue v

filterClause :: PersistEntity SqlBackend val
             => Bool -- ^ include table name?
             -> SqlBackend
             -> [Filter val]
             -> Text
filterClause b c = fst . filterClauseHelper b True c OrNullNo

orderClause :: PersistEntity SqlBackend val
            => Bool -- ^ include the table name
            -> SqlBackend
            -> SelectOpt val
            -> Text
orderClause includeTable conn o =
    case o of
        Asc  x -> name $ persistFieldDef x
        Desc x -> name (persistFieldDef x) <> " DESC"
        _ -> error $ "orderClause: expected Asc or Desc, not limit or offset"
  where
    dummyFromOrder :: SelectOpt a -> Maybe a
    dummyFromOrder _ = Nothing

    tn = connEscapeName conn $ entityDB $ entityDef $ dummyFromOrder o

    name x =
        (if includeTable
            then ((tn <> ".") <>)
            else id)
        $ connEscapeName conn $ fieldDB x

dummyFromKey :: Key v -> Maybe v
dummyFromKey _ = Nothing

-- | Generates sql for limit and offset for postgres, sqlite and mysql.
decorateSQLWithLimitOffset::Text -> (Int,Int) -> Bool -> Text -> Text 
decorateSQLWithLimitOffset nolimit (limit,offset) _ sql = 
    let
        lim = case (limit, offset) of
                (0, 0) -> ""
                (0, _) -> T.cons ' ' nolimit
                (_, _) -> " LIMIT " <> T.pack (show limit)
        off = if offset == 0
                    then ""
                    else " OFFSET " <> T.pack (show offset)
    in mconcat
            [ sql
            , lim
            , off
            ]            
