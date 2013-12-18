{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Sql.Orphan.PersistStore () where

import Database.Persist
import Database.Persist.Class.PersistStore
import Database.Persist.Sql.Types
import Database.Persist.Sql.Raw
import Database.Persist.Sql.Internal (convertKey)
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import Control.Monad.Reader (runReaderT)
import qualified Data.Text as T
import Data.Text (Text, unpack)
import Data.Monoid (mappend, (<>))
import Data.ByteString.Char8 (readInteger)
import Data.Maybe (isJust)
import Data.List (find)
import Control.Monad.Trans.Resource (with)

instance HasPersistBackend SqlBackend SqlBackend where
    persistBackend = id

instance PersistStoreImpl SqlBackend where
    insertImpl val conn = do
        let esql = connInsertSql conn t vals
        key <-
            case esql of
                ISRSingle sql -> with (rawQueryResource sql vals conn) $ \src -> src C.$$ do
                    x <- CL.head
                    case x of
                        Just [PersistInt64 i] -> return $ Key $ PersistInt64 i
                        Nothing -> error $ "SQL insert did not return a result giving the generated ID"
                        Just vals' -> error $ "Invalid result from a SQL insert, got: " ++ show vals'
                ISRInsertGet sql1 sql2 -> do
                    runReaderT (rawExecute sql1 vals) conn
                    with (rawQueryResource sql2 [] conn) $ \src -> src C.$$ do
                        mm <- CL.head
                        case mm of
                          Just [PersistInt64 i] -> return $ Key $ PersistInt64 i
                          Just [PersistDouble i] ->return $ Key $ PersistInt64 $ truncate i -- oracle need this!
                          Just [PersistByteString i] -> case readInteger i of -- mssql
                                                          Just (ret,"") -> return $ Key $ PersistInt64 $ fromIntegral ret
                                                          xs -> error $ "invalid number i["++show i++"] xs[" ++ show xs ++ "]"
                          Just xs -> error $ "invalid sql2 return xs["++show xs++"] sql2["++show sql2++"] sql1["++show sql1++"]"
                          Nothing -> error $ "invalid sql2 returned nothing sql2["++show sql2++"] sql1["++show sql1++"]"
                ISRManyKeys sql fs -> do
                    runReaderT (rawExecute sql vals) conn
                    case entityPrimary t of
                       Nothing -> error $ "ISRManyKeys is used when Primary is defined " ++ show sql
                       Just pdef -> 
                            let pks = map fst $ primaryFields pdef
                                keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) fs
                            in return $ Key $ PersistList keyvals

        return key
      where
        t = entityDef $ Just val
        vals = map toPersistValue $ toPersistFields val

    replaceImpl k val conn = do
        let t = entityDef $ Just val
        let sql = T.concat
                [ "UPDATE "
                , connEscapeName conn (entityDB t)
                , " SET "
                , T.intercalate "," (map (go . fieldDB) $ entityFields t)
                , " WHERE "
                , connEscapeName conn $ entityID t
                , "=?"
                ]
            vals = map toPersistValue (toPersistFields val) `mappend` [unKey k]
        runReaderT (rawExecute sql vals) conn
      where
        go x = connEscapeName conn x `T.append` "=?"

    insertKeyImpl = insrepHelper "INSERT"

    repsertImpl key value conn = do
        mExisting <- getImpl key conn
        case mExisting of
          Nothing -> insertKeyImpl key value conn
          Just _ -> replaceImpl key value conn

    getImpl k conn = do
        let t = entityDef $ dummyFromKey k
        let composite = isJust $ entityPrimary t
        let cols = T.intercalate ","
                 $ map (connEscapeName conn . fieldDB) $ entityFields t
        let wher = case entityPrimary t of
                     Just pdef -> T.intercalate " AND " $ map (\fld -> connEscapeName conn (snd fld) <> "=? ") $ primaryFields pdef
                     Nothing   -> connEscapeName conn (entityID t) <> "=?"
        let sql = T.concat
                [ "SELECT "
                , cols
                , " FROM "
                , connEscapeName conn $ entityDB t
                , " WHERE "
                , wher
                ]
        with (rawQueryResource sql (convertKey composite k) conn) $ \src -> src C.$$ do
            res <- CL.head
            case res of
                Nothing -> return Nothing
                Just vals ->
                    case fromPersistValues vals of
                        Left e -> error $ "get " ++ show (unKey k) ++ ": " ++ unpack e
                        Right v -> return $ Just v

    deleteImpl k conn = do
        runReaderT (rawExecute sql (convertKey composite k)) conn
      where
        t = entityDef $ dummyFromKey k
        composite = isJust $ entityPrimary t 
        wher = 
              case entityPrimary t of
                Just pdef -> T.intercalate " AND " $ map (\fld -> connEscapeName conn (snd fld) <> "=? ") $ primaryFields pdef
                Nothing   -> connEscapeName conn (entityID t) <> "=?"
        sql = T.concat
            [ "DELETE FROM "
            , connEscapeName conn $ entityDB t
            , " WHERE "
            , wher
            ]

dummyFromKey :: KeyBackend SqlBackend v -> Maybe v
dummyFromKey _ = Nothing

insrepHelper :: PersistEntity val
             => Text
             -> Key val
             -> val
             -> SqlBackend
             -> IO ()
insrepHelper command (Key k) val conn =
    runReaderT (rawExecute sql vals) conn
  where
    t = entityDef $ Just val
    sql = T.concat
        [ command
        , " INTO "
        , connEscapeName conn (entityDB t)
        , "("
        , T.intercalate ","
            $ map (connEscapeName conn)
            $ entityID t : map fieldDB (entityFields t)
        , ") VALUES("
        , T.intercalate "," ("?" : map (const "?") (entityFields t))
        , ")"
        ]
    vals = k : map toPersistValue (toPersistFields val)
