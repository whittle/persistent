{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
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

class PersistEntity a => DeleteCascade a where
    deleteCascadeImpl :: Key a -> PersistEntityBackend a -> IO ()

    deleteCascade :: PersistStore (PersistEntityBackend a) m
                  => Key a
                  -> m ()
    deleteCascade = runWithBackend . deleteCascadeImpl

deleteCascadeWhere :: (DeleteCascade a, PersistQuery backend m, backend ~ PersistEntityBackend a)
                   => [Filter a] -> m ()
deleteCascadeWhere filts = do
    backend <- askPersistBackend
    liftIO $ with (selectKeysImpl filts [] backend) (C.$$ CL.mapM_ (flip deleteCascadeImpl backend))
