{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies, FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Persist.Class.PersistStore
    ( PersistStore (..)
    , PersistStoreImpl (..)
    , HasPersistBackend (..)
    , getJust
    , belongsTo
    , belongsToJust
    ) where

import qualified Prelude
import Prelude hiding ((++), show)

import qualified Data.Text as T

import Control.Monad (liftM)
import Control.Monad.Trans.Class (lift)
import Data.Conduit (ConduitM, transPipe)
import Control.Monad.Reader (MonadReader (ask))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Exception.Lifted (throwIO)

import Database.Persist.Class.PersistEntity
import Database.Persist.Types

class PersistStoreImpl backend where
    -- | Get a record by identifier, if available.
    getImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => Key val -> backend -> IO (Maybe val)

    -- | Create a new record in the database, returning an automatically created
    -- key (in SQL an auto-increment id).
    insertImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => val -> backend -> IO (Key val)

    -- | Same as 'insert', but doesn't return a @Key@.
    insert_Impl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => val -> backend -> IO ()
    insert_Impl val backend = insertImpl val backend >> return ()

    -- | Create multiple records in the database.
    -- SQL backends currently use the slow default implementation of
    -- @mapM insert@
    insertManyImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => [val] -> backend -> IO [Key val]
    insertManyImpl vals backend = mapM (flip insertImpl backend) vals

    -- | Create a new record in the database using the given key.
    insertKeyImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => Key val -> val -> backend -> IO ()

    -- | Put the record in the database with the given key.
    -- Unlike 'replace', if a record with the given key does not
    -- exist then a new record will be inserted.
    repsertImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => Key val -> val -> backend -> IO ()

    -- | Replace the record in the database with the given
    -- key. Note that the result is undefined if such record does
    -- not exist, so you must use 'insertKey or 'repsert' in
    -- these cases.
    replaceImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => Key val -> val -> backend -> IO ()

    -- | Delete a specific record by identifier. Does nothing if record does
    -- not exist.
    deleteImpl
        :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => Key val -> backend -> IO ()

class PersistStoreImpl backend => HasPersistBackend env backend | env -> backend where
    persistBackend :: env -> backend

class (MonadIO m, PersistStoreImpl backend) => PersistStore backend m | m -> backend where
    runWithBackend :: (backend -> IO a) -> m a
    runWithBackend f = do
        backend <- askPersistBackend
        liftIO $ f backend

    askPersistBackend :: m backend

    -- | Get a record by identifier, if available.
    get :: (backend ~ PersistEntityBackend val, PersistEntity val)
        => Key val
        -> m (Maybe val)
    get = runWithBackend . getImpl

    -- | Create a new record in the database, returning an automatically created
    -- key (in SQL an auto-increment id).
    insert :: (backend ~ PersistEntityBackend val, PersistEntity val)
           => val
           -> m (Key val)
    insert = runWithBackend . insertImpl

    -- | Same as 'insert', but doesn't return a @Key@.
    insert_ :: (backend ~ PersistEntityBackend val, PersistEntity val)
            => val -> m ()
    insert_ = runWithBackend . insert_Impl

    -- | Create multiple records in the database.
    -- SQL backends currently use the slow default implementation of
    -- @mapM insert@
    insertMany :: (backend ~ PersistEntityBackend val, PersistEntity val)
               => [val] -> m [Key val]
    insertMany = runWithBackend . insertManyImpl

    -- | Create a new record in the database using the given key.
    insertKey :: (backend ~ PersistEntityBackend val, PersistEntity val)
              => Key val -> val -> m ()
    insertKey k = runWithBackend . insertKeyImpl k

    -- | Put the record in the database with the given key.
    -- Unlike 'replace', if a record with the given key does not
    -- exist then a new record will be inserted.
    repsert :: (backend ~ PersistEntityBackend val, PersistEntity val)
            => Key val -> val -> m ()
    repsert k = runWithBackend . repsertImpl k

    -- | Replace the record in the database with the given
    -- key. Note that the result is undefined if such record does
    -- not exist, so you must use 'insertKey or 'repsert' in
    -- these cases.
    replace :: (backend ~ PersistEntityBackend val, PersistEntity val)
            => Key val -> val -> m ()
    replace k = runWithBackend . replaceImpl k

    -- | Delete a specific record by identifier. Does nothing if record does
    -- not exist.
    delete :: (backend ~ PersistEntityBackend val, PersistEntity val)
           => Key val -> m ()
    delete = runWithBackend . deleteImpl

instance (MonadIO m, MonadReader env m, HasPersistBackend env backend) => PersistStore backend m where
    runWithBackend f = do
        env <- ask
        liftIO $ f $ persistBackend env

    askPersistBackend = liftM persistBackend ask

-- | Same as get, but for a non-null (not Maybe) foreign key
--   Unsafe unless your database is enforcing that the foreign key is valid
getJust :: (PersistStore backend m, PersistEntity val, Show (Key val), backend ~ PersistEntityBackend val) => Key val -> m val
getJust key = get key >>= maybe
  (liftIO $ throwIO $ PersistForeignConstraintUnmet $ T.pack $ Prelude.show key)
  return

-- | curry this to make a convenience function that loads an associated model
--   > foreign = belongsTo foeignId
belongsTo ::
  (PersistStore backend m
  , PersistEntity ent1
  , PersistEntity ent2
  , backend ~ PersistEntityBackend ent2
  ) => (ent1 -> Maybe (Key ent2)) -> ent1 -> m (Maybe ent2)
belongsTo foreignKeyField model = case foreignKeyField model of
    Nothing -> return Nothing
    Just f -> get f

-- | same as belongsTo, but uses @getJust@ and therefore is similarly unsafe
belongsToJust ::
  (PersistStore backend m
  , PersistEntity ent1
  , PersistEntity ent2
  , backend ~ PersistEntityBackend ent2)
  => (ent1 -> Key ent2) -> ent1 -> m ent2
belongsToJust getForeignKey model = getJust $ getForeignKey model
