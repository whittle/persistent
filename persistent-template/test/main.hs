{-# LANGUAGE OverloadedStrings, QuasiQuotes, TemplateHaskell, TypeFamilies, GADTs #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
module Main
  (
  -- avoid unused ident warnings
    module Main
  ) where
import Test.Hspec
import Test.Hspec.QuickCheck
import Data.ByteString.Lazy.Char8 ()
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Gen (Gen)
import Control.Applicative as A ((<$>), (<*>), Const (..))
import Data.Functor.Identity (Identity (..))

import Database.Persist
import Database.Persist.TH
import Data.Text (Text, pack)
import Data.Aeson

share [mkPersist sqlSettings { mpsGeneric = False }, mkDeleteCascade sqlSettings { mpsGeneric = False }] [persistUpperCase|
Person json
    name Text
    age Int Maybe
    address Address
    deriving Show Eq
Address json
    street Text
    city Text
    zip Int Maybe
    deriving Show Eq
NoJson
    foo Text
    deriving Show Eq
|]

share [mkPersist sqlSettings { mpsGeneric = False, mpsGenerateLenses = True }] [persistLowerCase|
Lperson json
    name Text
    age Int Maybe
    address Laddress
    deriving Show Eq
Laddress json
    street Text
    city Text
    zip Int Maybe
    deriving Show Eq
|]

share [mkPersist sqlSettings { mpsGeneric = True , mpsGenerateLenses = True }] [persistLowerCase|
Foo
    bar Text Maybe
    baz BazId
Baz
    bin Int
|]

share [mkPersist sqlSettings { mpsEntityJSON = Just entityJSONWithAuto, mpsGenerateLenses = True }] [persistLowerCase|
User json
    authSubject Text
    %roleCount Int
    deriving Show Eq
Role
    userId UserId
    name Text
    deriving Show Eq
|]

arbitraryT :: Gen Text
arbitraryT = pack A.<$> arbitrary

instance Arbitrary Person where
    arbitrary = Person <$> arbitraryT A.<*> arbitrary <*> arbitrary
instance Arbitrary Address where
    arbitrary = Address <$> arbitraryT <*> arbitraryT <*> arbitrary
instance Arbitrary User where
    arbitrary = User <$> arbitraryT
instance Arbitrary (Auto User) where
    arbitrary = UserAuto <$> arbitrary

main :: IO ()
main = hspec $ do
    describe "JSON serialization" $ do
        prop "to/from is idempotent" $ \person ->
            decode (encode person) == Just (person :: Person)
        it "decode" $
            decode "{\"name\":\"Michael\",\"age\":27,\"address\":{\"street\":\"Narkis\",\"city\":\"Maalot\"}}" `shouldBe` Just
                (Person "Michael" (Just 27) $ Address "Narkis" "Maalot" Nothing)
    describe "JSON serialization for Entity" $ do
        let key = PersonKey 0
        prop "to/from is idempotent" $ \person ->
            decode (encode (Entity key person Nothing)) == Just (Entity key (person :: Person) Nothing)
        it "decode" $
            decode "{\"id\": 0, \"name\":\"Michael\",\"age\":27,\"address\":{\"street\":\"Narkis\",\"city\":\"Maalot\"}}" `shouldBe` Just
                (Entity key (Person "Michael" (Just 27) $ Address "Narkis" "Maalot" Nothing) Nothing)
    describe "JSON serialization for Entity with auto-columns" $ do
        let key = UserKey 0
        prop "to/from is idempotent" $ \(user, mUserAuto) ->
            decode (encode (Entity key user mUserAuto)) == Just (Entity key user mUserAuto)
        it "decode" $ do
            decode "{\"id\": 0, \"authSubject\":\"foo\", \"roleCount\":2}" `shouldBe` Just
                (Entity key (User "foo") (Just (UserAuto 2)))
            decode "{\"id\": 0, \"authSubject\":\"bar\"}" `shouldBe` Just
                (Entity key (User "bar") Nothing)
    it "lens operations" $ do
        let street1 = "street1"
            city1 = "city1"
            city2 = "city2"
            zip1 = Just 12345
            address1 = Laddress street1 city1 zip1
            address2 = Laddress street1 city2 zip1
            name1 = "name1"
            age1 = Just 27
            person1 = Lperson name1 age1 address1
            person2 = Lperson name1 age1 address2
        (person1 ^. lpersonAddress) `shouldBe` address1
        (person1 ^. (lpersonAddress . laddressCity)) `shouldBe` city1
        (person1 & ((lpersonAddress . laddressCity) .~ city2)) `shouldBe` person2
        let userA = UserAuto 2
        (userA ^. userRoleCount) `shouldBe` 2


(&) :: a -> (a -> b) -> b
x & f = f x

(^.) :: s
     -> ((a -> Const a b) -> (s -> Const a t))
     -> a
x ^. lens = getConst $ lens Const x

(.~) :: ((a -> Identity b) -> (s -> Identity t))
     -> b
     -> s
     -> t
lens .~ val = runIdentity . lens (\_ -> Identity val)
