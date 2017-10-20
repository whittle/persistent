{-# LANGUAGE OverloadedStrings #-}
module Database.Persist.Sql.Util (
    parseEntityValues
  , entityColumnNames
  , keyAndEntityColumnNames
  , entityColumnCount
  , isIdField
  , hasCompositeKey
  , dbIdColumns
  , dbIdColumnsEsc
  , dbColumns
) where

import Data.Maybe (isJust)
import Data.Monoid ((<>))
import Data.Text (Text, pack)
import Database.Persist (
    Entity(Entity), EntityDef, EntityField, HaskellName(HaskellName)
  , PersistEntity, PersistValue
  , keyFromValues, fromPersistValues, fromAutoPersistValues, fieldDB, entityId
  , entityPrimary, entityFields, entityKeyFields, fieldHaskell, compositeFields
  , persistFieldDef, keyAndEntityFields, entityFieldsAndAutos
  , DBName)
import Database.Persist.Sql.Types (Sql, SqlBackend, connEscapeName)

entityColumnNames :: EntityDef -> SqlBackend -> [Sql]
entityColumnNames ent conn =
     (if hasCompositeKey ent
      then [] else [connEscapeName conn $ fieldDB (entityId ent)])
  <> map (connEscapeName conn . fieldDB) (entityFieldsAndAutos ent)

keyAndEntityColumnNames :: EntityDef -> SqlBackend -> [Sql]
keyAndEntityColumnNames ent conn = map (connEscapeName conn . fieldDB) (keyAndEntityFields ent)

entityColumnCount :: EntityDef -> Int
entityColumnCount e = length (entityFieldsAndAutos e)
                    + if hasCompositeKey e then 0 else 1

hasCompositeKey :: EntityDef -> Bool
hasCompositeKey = isJust . entityPrimary

dbIdColumns :: SqlBackend -> EntityDef -> [Text]
dbIdColumns conn = dbIdColumnsEsc (connEscapeName conn)

dbIdColumnsEsc :: (DBName -> Text) -> EntityDef -> [Text]
dbIdColumnsEsc esc t = map (esc . fieldDB) $ entityKeyFields t

dbColumns :: SqlBackend -> EntityDef -> [Text]
dbColumns conn t = case entityPrimary t of
    Just _  -> flds
    Nothing -> escapeDB (entityId t) : flds
  where
    escapeDB = connEscapeName conn . fieldDB
    flds = map escapeDB (entityFieldsAndAutos t)

parseEntityValues :: PersistEntity record
                  => EntityDef -> [PersistValue] -> Either Text (Entity record)
parseEntityValues t vals =
    case entityPrimary t of
      Just pdef ->
            let pks = map fieldHaskell $ compositeFields pdef
                keyvals = map snd . filter ((`elem` pks) . fst)
                        $ zip (map fieldHaskell $ entityFields t) vals
            in fromPersistValuesComposite' keyvals vals
      Nothing -> fromPersistValues' vals
  where
    fromPersistValues' (kpv:xs) = -- oracle returns Double
        let (vs, as) = flip splitAt xs $ length $ entityFields t
        in case (fromPersistValues vs, fromAutoPersistValues as) of
            (Left e, _) -> Left e
            (_, Left e) -> Left e
            (Right xs', Right xs'') -> case keyFromValues [kpv] of
                Left _ -> error $ "fromPersistValues': keyFromValues failed on " ++ show kpv
                Right k -> Right (Entity k xs' (Just xs''))


    fromPersistValues' xs = Left $ pack ("error in fromPersistValues' xs=" ++ show xs)

    fromPersistValuesComposite' keyvals xs =
        let (vs, as) = flip splitAt xs $ length $ entityFields t
        in case (fromPersistValues vs, fromAutoPersistValues as) of
            (Left e, _) -> Left e
            (_, Left e) -> Left e
            (Right xs', Right xs'') -> case keyFromValues keyvals of
                Left _ -> error "fromPersistValuesComposite': keyFromValues failed"
                Right key -> Right (Entity key xs' (Just xs''))


isIdField :: PersistEntity record => EntityField record typ -> Bool
isIdField f = fieldHaskell (persistFieldDef f) == HaskellName "Id"
