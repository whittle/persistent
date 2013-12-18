{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Persist.Class.PersistQuery
    ( PersistQuery (..)
    , PersistQueryImpl (..)
    , selectList
    , selectKeysList
    , selectSource
    , selectKeys
    ) where

import Control.Exception (throwIO)
import Database.Persist.Types
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)

import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import Database.Persist.Class.PersistStore
import Database.Persist.Class.PersistEntity
import Control.Monad.Trans.Resource (Resource, allocateResource, release, with)
import Control.Monad.Reader (MonadReader)

class PersistStoreImpl backend => PersistQueryImpl backend where
    -- | Update individual fields on a specific record.
    updateImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => Key val -> [Update val] -> backend -> IO ()

    -- | Update individual fields on a specific record, and retrieve the
    -- updated value from the database.
    --
    -- Note that this function will throw an exception if the given key is not
    -- found in the database.
    updateGetImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => Key val -> [Update val] -> backend -> IO val
    updateGetImpl key ups backend = do
        updateImpl key ups backend
        getImpl key backend >>= maybe (throwIO $ KeyNotFound $ show key) return

    -- | Update individual fields on any record matching the given criterion.
    updateWhereImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val] -> [Update val] -> backend -> IO ()

    -- | Delete all records matching the given criterion.
    deleteWhereImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val] -> backend -> IO ()

    -- | Get all records matching the given criterion in the specified order.
    -- Returns also the identifiers.
    selectSourceImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val]
        -> [SelectOpt val]
        -> backend
        -> Resource (C.Source IO (Entity val))

    -- | get just the first record for the criterion
    selectFirstImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val]
        -> [SelectOpt val]
        -> backend
        -> IO (Maybe (Entity val))
    selectFirstImpl filts opts backend =
        with (selectSourceImpl filts ((LimitTo 1):opts) backend) (C.$$ CL.head)

    -- | Get the 'Key's of all records matching the given criterion.
    selectKeysImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val]
        -> [SelectOpt val]
        -> backend
        -> Resource (C.Source IO (Key val))

    -- | The total number of records fulfilling the given criterion.
    countImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val]
        -> backend
        -> IO Int

class (PersistStore backend m, PersistQueryImpl backend) => PersistQuery backend m | m -> backend where
    -- | Update individual fields on a specific record.
    update :: (PersistEntity val, backend ~ PersistEntityBackend val)
           => Key val -> [Update val] -> m ()
    update k = runWithBackend . updateImpl k

    -- | Update individual fields on a specific record, and retrieve the
    -- updated value from the database.
    --
    -- Note that this function will throw an exception if the given key is not
    -- found in the database.
    updateGet :: (PersistEntity val, backend ~ PersistEntityBackend val)
              => Key val -> [Update val] -> m val
    updateGet k = runWithBackend . updateGetImpl k

    -- | Update individual fields on any record matching the given criterion.
    updateWhere :: (PersistEntity val, backend ~ PersistEntityBackend val)
                => [Filter val] -> [Update val] -> m ()
    updateWhere k = runWithBackend . updateWhereImpl k

    -- | Delete all records matching the given criterion.
    deleteWhere :: (PersistEntity val, backend ~ PersistEntityBackend val)
                => [Filter val] -> m ()
    deleteWhere = runWithBackend . deleteWhereImpl

    -- | get just the first record for the criterion
    selectFirst :: (PersistEntity val, backend ~ PersistEntityBackend val)
                => [Filter val]
                -> [SelectOpt val]
                -> m (Maybe (Entity val))
    selectFirst f = runWithBackend . selectFirstImpl f

    -- | The total number of records fulfilling the given criterion.
    count :: (PersistEntity val, backend ~ PersistEntityBackend val)
          => [Filter val] -> m Int
    count = runWithBackend . countImpl

runConduitWithBackend :: (PersistStoreImpl backend, C.MonadResource m, HasPersistBackend env backend, MonadReader env m)
                      => (backend -> Resource (C.ConduitM a b IO c))
                      -> C.ConduitM a b m c
runConduitWithBackend f = do
    backend <- lift askPersistBackend
    (releaseKey, src) <- allocateResource $ f backend
    c <- C.transPipe liftIO src
    release releaseKey
    return c

-- | Get all records matching the given criterion in the specified order.
-- Returns also the identifiers.
selectSource
       :: (PersistEntity val, backend ~ PersistEntityBackend val, PersistQueryImpl backend, C.MonadResource m, HasPersistBackend env backend, MonadReader env m)
       => [Filter val]
       -> [SelectOpt val]
       -> C.Source m (Entity val)
selectSource f = runConduitWithBackend . selectSourceImpl f

-- | Get the 'Key's of all records matching the given criterion.
selectKeys :: (PersistEntity val, backend ~ PersistEntityBackend val, PersistQueryImpl backend, C.MonadResource m, HasPersistBackend env backend, MonadReader env m)
           => [Filter val]
           -> [SelectOpt val]
           -> C.Source m (Key val)
selectKeys f = runConduitWithBackend . selectKeysImpl f

instance (PersistStore backend m, PersistQueryImpl backend) => PersistQuery backend m

-- | Call 'selectSource' but return the result as a list.
selectList :: (PersistEntity val, PersistQuery backend m, backend ~ PersistEntityBackend val)
           => [Filter val]
           -> [SelectOpt val]
           -> m [Entity val]
selectList a b = do
    backend <- askPersistBackend
    liftIO $ with (selectSourceImpl a b backend) (C.$$ CL.consume)

-- | Call 'selectKeys' but return the result as a list.
selectKeysList :: (PersistEntity val, PersistQuery backend m, backend ~ PersistEntityBackend val)
               => [Filter val]
               -> [SelectOpt val]
               -> m [Key val]
selectKeysList a b = do
    backend <- askPersistBackend
    liftIO $ with (selectKeysImpl a b backend) (C.$$ CL.consume)
