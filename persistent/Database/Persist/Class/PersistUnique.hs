{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Persist.Class.PersistUnique
    ( PersistUniqueImpl (..)
    , PersistUnique (..)
    , getByValue
    , insertBy
    , replaceUnique
    , checkUnique
    ) where

import qualified Prelude
import Prelude hiding ((++), show)

import Control.Monad (liftM)
import Data.List ((\\))

import Database.Persist.Class.PersistStore
import Database.Persist.Class.PersistEntity

class PersistStoreImpl backend => PersistUniqueImpl backend where
    -- | Get a record by unique key, if available. Returns also the identifier.
    getByImpl :: PersistEntity backend val
              => Unique val
              -> backend
              -> IO (Maybe (Entity val))

    -- | Delete a specific record by unique key. Does nothing if no record
    -- matches.
    deleteByImpl :: PersistEntity backend val
                 => Unique val
                 -> backend
                 -> IO ()

    -- | Like 'insert', but returns 'Nothing' when the record
    -- couldn't be inserted because of a uniqueness constraint.
    insertUniqueImpl
        :: PersistEntity backend val
        => val
        -> backend
        -> IO (Maybe (Key val))
    insertUniqueImpl datum backend = do
        conflict <- checkUniqueImpl datum backend
        case conflict of
          Nothing -> Just `liftM` insertImpl datum backend
          Just _ -> return Nothing

-- | Queries against unique keys (other than the id).
--
-- Please read the general Persistent documentation to learn how to create
-- Unique keys.
-- SQL backends automatically create uniqueness constraints, but for MongoDB you must manually place a unique index on the field.
--
-- Some functions in this module (insertUnique, insertBy, and replaceUnique) first query the unique indexes to check for conflicts.
-- You could instead optimistically attempt to perform the operation (e.g. replace instead of replaceUnique). However,
--
--  * there is some fragility to trying to catch the correct exception and determing the column of failure.
--
--  * an exception will automatically abort the current SQL transaction
class (PersistStore backend m, PersistUniqueImpl backend) => PersistUnique backend m | m -> backend where
    -- | Get a record by unique key, if available. Returns also the identifier.
    getBy :: PersistEntity backend val => Unique val -> m (Maybe (Entity val))
    getBy = runWithBackend . getByImpl

    -- | Delete a specific record by unique key. Does nothing if no record
    -- matches.
    deleteBy :: PersistEntity backend val => Unique val -> m ()
    deleteBy = runWithBackend . deleteByImpl

    -- | Like 'insert', but returns 'Nothing' when the record
    -- couldn't be inserted because of a uniqueness constraint.
    insertUnique :: PersistEntity backend val => val -> m (Maybe (Key val))
    insertUnique = runWithBackend . insertUniqueImpl

instance (PersistStore backend m, PersistUniqueImpl backend) => PersistUnique backend m

-- | Insert a value, checking for conflicts with any unique constraints.  If a
-- duplicate exists in the database, it is returned as 'Left'. Otherwise, the
-- new 'Key is returned as 'Right'.
insertBy :: (PersistEntity backend val, PersistUnique backend m)
         => val -> m (Either (Entity val) (Key val))
insertBy val = do
    res <- getByValue val
    case res of
      Nothing -> Right `liftM` insert val
      Just z -> return $ Left z

-- | A modification of 'getBy', which takes the 'PersistEntity' itself instead
-- of a 'Unique' value. Returns a value matching /one/ of the unique keys. This
-- function makes the most sense on entities with a single 'Unique'
-- constructor.
getByValue :: (PersistEntity backend value, PersistUnique backend m)
           => value -> m (Maybe (Entity value))
getByValue = checkUniques . persistUniqueKeys

checkUniques :: (PersistEntity backend value, PersistUnique backend m)
             => [Unique value]
             -> m (Maybe (Entity value))
checkUniques [] = return Nothing
checkUniques (x:xs) = do
    y <- getBy x
    case y of
        Nothing -> checkUniques xs
        Just z -> return $ Just z


-- | Attempt to replace the record of the given key with the given new record.
-- First query the unique fields to make sure the replacement maintains uniqueness constraints.
-- Return 'Nothing' if the replacement was made.
-- If uniqueness is violated, return a 'Just' with the 'Unique' violation
--
-- Since 1.2.2.0
replaceUnique :: (Eq record, Eq (Unique record), PersistEntity backend record, PersistUnique backend m)
              => Key record -> record -> m (Maybe (Unique record))
replaceUnique key datumNew = getJust key >>= replaceOriginal
  where
    uniqueKeysNew = persistUniqueKeys datumNew
    replaceOriginal original = do
        conflict <- checkUniqueKeys changedKeys
        case conflict of
          Nothing -> replace key datumNew >> return Nothing
          (Just conflictingKey) -> return $ Just conflictingKey
      where
        changedKeys = uniqueKeysNew \\ uniqueKeysOriginal
        uniqueKeysOriginal = persistUniqueKeys original

-- | Check whether there are any conflicts for unique keys with this entity and
-- existing entities in the database.
--
-- Returns 'Nothing' if the entity would be unique, and could thus safely be inserted.
-- on a conflict returns the conflicting key
checkUnique :: (PersistEntity backend record, PersistUnique backend m)
            => record -> m (Maybe (Unique record))
checkUnique = runWithBackend . checkUniqueImpl

checkUniqueImpl :: (PersistEntity backend record, PersistUniqueImpl backend)
                => record
                -> backend
                -> IO (Maybe (Unique record))
checkUniqueImpl = checkUniqueKeysImpl . persistUniqueKeys

checkUniqueKeys :: (PersistEntity backend record, PersistUnique backend m)
                => [Unique record] -> m (Maybe (Unique record))
checkUniqueKeys = runWithBackend . checkUniqueKeysImpl

checkUniqueKeysImpl
    :: (PersistEntity backend record, PersistUniqueImpl backend)
    => [Unique record]
    -> backend
    -> IO (Maybe (Unique record))
checkUniqueKeysImpl [] _ = return Nothing
checkUniqueKeysImpl (x:xs) backend = do
    y <- getByImpl x backend
    case y of
        Nothing -> checkUniqueKeysImpl xs backend
        Just _ -> return (Just x)
