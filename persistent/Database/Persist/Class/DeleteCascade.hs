{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
module Database.Persist.Class.DeleteCascade
    ( DeleteCascade (..)
    , deleteCascadeWhere
    ) where

import Database.Persist.Class.PersistStore
import Database.Persist.Class.PersistQuery
import Database.Persist.Class.PersistEntity

import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL

class (PersistStore m, PersistEntity record) => DeleteCascade record m where
    deleteCascade :: MonadDb m ~ db => IKey record db -> m ()

deleteCascadeWhere :: (DeleteCascade record m, PersistQuery m, MonadDb m ~ db)
                   => [Filter record db] -> m ()
deleteCascadeWhere filts = selectKeys filts [] C.$$ CL.mapM_ deleteCascade
