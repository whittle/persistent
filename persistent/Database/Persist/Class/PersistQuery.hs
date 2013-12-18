{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Persist.Class.PersistQuery
    ( PersistQuery (..)
    , selectList
    , selectKeysList
    ) where

import Control.Exception (throwIO)
import Database.Persist.Types

import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import Database.Persist.Class.PersistStore
import Database.Persist.Class.PersistEntity

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
        -> C.Source IO (Entity val)

    -- | get just the first record for the criterion
    selectFirstImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val]
        -> [SelectOpt val]
        -> backend
        -> IO (Maybe (Entity val))
    selectFirstImpl filts opts backend = selectSourceImpl filts ((LimitTo 1):opts) backend C.$$ CL.head

    -- | Get the 'Key's of all records matching the given criterion.
    selectKeysImpl
        :: (PersistEntity val, backend ~ PersistEntityBackend val)
        => [Filter val]
        -> [SelectOpt val]
        -> backend
        -> C.Source IO (Key val)

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

    -- | Get all records matching the given criterion in the specified order.
    -- Returns also the identifiers.
    selectSource
           :: (PersistEntity val, backend ~ PersistEntityBackend val)
           => [Filter val]
           -> [SelectOpt val]
           -> C.Source m (Entity val)
    selectSource f = runConduitWithBackend . selectSourceImpl f

    -- | get just the first record for the criterion
    selectFirst :: (PersistEntity val, backend ~ PersistEntityBackend val)
                => [Filter val]
                -> [SelectOpt val]
                -> m (Maybe (Entity val))
    selectFirst f = runWithBackend . selectFirstImpl f

    -- | Get the 'Key's of all records matching the given criterion.
    selectKeys :: (PersistEntity val, backend ~ PersistEntityBackend val)
               => [Filter val]
               -> [SelectOpt val]
               -> C.Source m (Key val)
    selectKeys f = runConduitWithBackend . selectKeysImpl f

    -- | The total number of records fulfilling the given criterion.
    count :: (PersistEntity val, backend ~ PersistEntityBackend val)
          => [Filter val] -> m Int
    count = runWithBackend . countImpl

instance (PersistStore backend m, PersistQueryImpl backend) => PersistQuery backend m

-- | Call 'selectSource' but return the result as a list.
selectList :: (PersistEntity val, PersistQuery backend m, backend ~ PersistEntityBackend val)
           => [Filter val]
           -> [SelectOpt val]
           -> m [Entity val]
selectList a b = selectSource a b C.$$ CL.consume

-- | Call 'selectKeys' but return the result as a list.
selectKeysList :: (PersistEntity val, PersistQuery backend m, backend ~ PersistEntityBackend val)
               => [Filter val]
               -> [SelectOpt val]
               -> m [Key val]
selectKeysList a b = selectKeys a b C.$$ CL.consume
