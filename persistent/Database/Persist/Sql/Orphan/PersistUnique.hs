{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Sql.Orphan.PersistUnique () where

import Database.Persist
import Database.Persist.Class.PersistUnique
import Database.Persist.Sql.Types
import Database.Persist.Sql.Class
import Database.Persist.Sql.Raw
import Database.Persist.Sql.Orphan.PersistStore ()
import qualified Data.Text as T
import Data.Monoid ((<>))
import Control.Monad.Logger
import Control.Monad.Trans.Resource (with)
import Control.Monad.Reader (runReaderT)
import qualified Data.Conduit.List as CL
import Data.Conduit

instance PersistUniqueImpl SqlBackend where
    deleteByImpl uniq conn = do
        let sql' = sql conn
            vals = persistUniqueToValues uniq
        runReaderT (rawExecute sql' vals) conn
      where
        t = entityDef $ dummyFromUnique uniq
        go = map snd . persistUniqueToFieldNames
        go' conn x = connEscapeName conn x <> "=?"
        sql conn = T.concat
            [ "DELETE FROM "
            , connEscapeName conn $ entityDB t
            , " WHERE "
            , T.intercalate " AND " $ map (go' conn) $ go uniq
            ]

    getByImpl uniq conn = do
        let flds = map (connEscapeName conn . fieldDB) (entityFields t)
        let cols = case entityPrimary t of
                     Just _ -> T.intercalate "," flds
                     Nothing -> T.intercalate "," $ (connEscapeName conn $ entityID t) : flds
        let sql = T.concat
                [ "SELECT "
                , cols
                , " FROM "
                , connEscapeName conn $ entityDB t
                , " WHERE "
                , sqlClause conn
                ]
            vals' = persistUniqueToValues uniq
        with (rawQueryResource sql vals' conn) $ \src -> src $$ do -- FIXME broken for composite keys
            row <- CL.head
            case row of
                Nothing -> return Nothing
                Just (k:vals) ->
                    case fromPersistValues vals of
                        Left s -> error $ T.unpack s
                        Right x ->
                            case keyFromValues [k] of
                                Just k' -> return $ Just (Entity k' x)
                                Nothing -> error "getByImpl: keyFromValues failed"
                Just xs -> error $ "Database.Persist.GenericSql: Bad list in getBy xs="++show xs
      where
        sqlClause conn =
            T.intercalate " AND " $ map (go conn) $ toFieldNames' uniq
        go conn x = connEscapeName conn x <> "=?"
        t = entityDef $ dummyFromUnique uniq
        toFieldNames' = map snd . persistUniqueToFieldNames

dummyFromUnique :: Unique v -> Maybe v
dummyFromUnique _ = Nothing
