{-# LANGUAGE QuasiQuotes, TemplateHaskell, CPP, GADTs, TypeFamilies, OverloadedStrings, FlexibleContexts, EmptyDataDecls, FlexibleInstances, GeneralizedNewtypeDeriving, MultiParamTypeClasses #-}
module DbSpecificTest where

import Init

data Geo = Geo ByteString

instance PersistField Geo where
  toPersistValue (Geo t) = PersistDbSpecific t

  fromPersistValue (PersistDbSpecific t) = Right $ Geo $ Data.ByteString.concat ["'", t, "'"]
  fromPersistValue _ = Left "Geo values must be converted from PersistDbSpecific"

instance PersistFieldSql Geo where
  sqlType _ = SqlOther "GEOGRAPHY(POINT,4326)"

toPoint :: Double -> Double -> Geo
toPoint lat lon = Geo $ Data.ByteString.concat ["'POINT(", ps $ lon, " ", ps $ lat, ")'"]
  where ps = Data.Text.pack . show

#if WITH_NOSQL
mkPersist persistSettings { mpsGeneric = False } [persistUpperCase|
#else
share [mkPersist persistSettings { mpsGeneric = False }, mkMigrate "migration"] [persistLowerCase|
#endif
  Specific
      geo Geo
      deriving Eq Show
|]

#ifdef WITH_NOSQL
cleanDB :: (MonadIO m, PersistQuery backend, PersistEntityBackend Fo ~ backend) => ReaderT backend m ()
cleanDB = do
  deleteWhere ([] :: [Filter Specific])

db :: Action IO () -> Assertion
db = db' cleanDB
#endif

specs :: Spec
specs = describe "db specific type" $ db $ do
  insert $ Specific $ toPoint 44 44
