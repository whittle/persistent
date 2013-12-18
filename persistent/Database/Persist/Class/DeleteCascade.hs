{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
module Database.Persist.Class.DeleteCascade
    ( DeleteCascade (..)
    , deleteCascadeWhere
    ) where

import Database.Persist.Class.PersistStore
import Database.Persist.Class.PersistQuery
import Database.Persist.Class.PersistEntity

import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import Control.Monad.Trans.Resource (with)
import Control.Monad.IO.Class (liftIO)

class PersistEntity backend entity => DeleteCascade backend entity | entity -> backend where
    deleteCascadeImpl :: Key entity -> backend -> IO ()

    deleteCascade :: PersistStore backend m
                  => Key entity
                  -> m ()
    deleteCascade = runWithBackend . deleteCascadeImpl

deleteCascadeWhere :: (DeleteCascade backend entity, PersistQuery backend m)
                   => [Filter entity] -> m ()
deleteCascadeWhere filts = do
    backend <- askPersistBackend
    liftIO $ with (selectKeysImpl filts [] backend) (C.$$ CL.mapM_ (flip deleteCascadeImpl backend))
