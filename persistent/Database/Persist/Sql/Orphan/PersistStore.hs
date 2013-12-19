{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Sql.Orphan.PersistStore () where

import Database.Persist
import Database.Persist.Class.PersistStore
import Database.Persist.Sql.Types
import Database.Persist.Sql.Class (PersistFieldSql)
import Web.PathPieces (PathPiece)
import Database.Persist.Sql.Raw
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
import Data.Int (Int64)

instance HasPersistBackend SqlBackend SqlBackend where
    persistBackend = id

instance PersistStoreImpl SqlBackend where
    newtype BackendKey SqlBackend = SqlBackendKey Int64
        deriving (Show, Read, Eq, Ord, Num, Integral, PersistField, PersistFieldSql, PathPiece, Real, Enum, Bounded)

    backendKeyToValues (SqlBackendKey i) = [PersistInt64 i]

    backendKeyFromValues [PersistInt64 i] = Just $ SqlBackendKey i -- FIXME add support for more types of keys
    backendKeyFromValues [PersistDouble i] = Just $ SqlBackendKey $ truncate i
    backendKeyFromValues _ = Nothing

    insertImpl val conn = do
        let esql = connInsertSql conn t vals
        key <-
            case esql of
                ISRSingle sql -> with (rawQueryResource sql vals conn) $ \src -> src C.$$ do
                    x <- CL.head
                    case x of
                        Nothing -> error $ "SQL insert did not return a result giving the generated ID"
                        Just vals' ->
                            case keyFromValues vals' of
                                Nothing -> error $ "Invalid result from a SQL insert, got: " ++ show vals'
                                Just k -> return k
                ISRInsertGet sql1 sql2 -> do
                    runReaderT (rawExecute sql1 vals) conn
                    with (rawQueryResource sql2 [] conn) $ \src -> src C.$$ do
                        mm <- CL.head
                        i <- case mm of
                          Just [PersistInt64 i] -> return i
                          Just [PersistDouble i] ->return $ truncate i -- oracle need this!
                          Just [PersistByteString i] ->
                            case readInteger i of -- mssql
                              Just (ret,"") -> return $ fromIntegral ret
                              xs -> error $ "invalid number i["++show i++"] xs[" ++ show xs ++ "]"
                          Just xs -> error $ "invalid sql2 return xs["++show xs++"] sql2["++show sql2++"] sql1["++show sql1++"]"
                          Nothing -> error $ "invalid sql2 returned nothing sql2["++show sql2++"] sql1["++show sql1++"]"
                        case keyFromValues [PersistInt64 i] of
                            Just k -> return k
                            Nothing -> error "ISRInsertGet: keyFromValues failed"
                ISRManyKeys sql fs -> do
                    runReaderT (rawExecute sql vals) conn
                    case entityPrimary t of
                       Nothing -> error $ "ISRManyKeys is used when Primary is defined " ++ show sql
                       Just pdef -> 
                            let pks = map fst $ primaryFields pdef
                                keyvals = map snd $ filter (\(a, _) -> let ret=isJust (find (== a) pks) in ret) $ zip (map fieldHaskell $ entityFields t) fs
                            in case keyFromValues keyvals of
                                   Just k -> return k
                                   Nothing -> error "ISRManyKeys: unexpected keyvals result"

        return key
      where
        t = entityDef $ Just val
        vals = map toPersistValue $ toPersistFields val

    replaceImpl k val conn = do -- FIXME broken for composites
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
            vals = map toPersistValue (toPersistFields val) `mappend` keyToValues k
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
        with (rawQueryResource sql (keyToValues k) conn) $ \src -> src C.$$ do
            res <- CL.head
            case res of
                Nothing -> return Nothing
                Just vals ->
                    case fromPersistValues vals of
                        Left e -> error $ "get " ++ show k ++ ": " ++ unpack e
                        Right v -> return $ Just v

    deleteImpl k conn = do
        runReaderT (rawExecute sql (keyToValues k)) conn
      where
        t = entityDef $ dummyFromKey k
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

dummyFromKey :: Key v -> Maybe v
dummyFromKey _ = Nothing

insrepHelper :: PersistEntity SqlBackend val
             => Text
             -> Key val
             -> val
             -> SqlBackend
             -> IO ()
insrepHelper command k val conn = -- FIXME this function is broken for composite keys
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
    vals = keyToValues k ++ map toPersistValue (toPersistFields val)
