{-# LANGUAGE FlexibleContexts #-}
module Database.Persist.Sql.Run where

import Database.Persist.Sql.Types
import Database.Persist.Sql.Raw
import Data.Conduit.Pool
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Resource
import Control.Monad.Base
import Control.Exception.Lifted (onException)
import Control.Monad.IO.Class
import Control.Exception.Lifted (bracket)
import Data.IORef (readIORef)
import qualified Data.Map as Map
import Control.Exception.Lifted (throwIO)
import Control.Monad.Logger (LogFunc, MonadLogger (askLogFunc))

-- | Get a connection from the pool, run the given action, and then return the
-- connection to the pool.
runSqlPool :: MonadBaseControl IO m => SqlPersistT m a -> Pool SqlBackend -> m a
runSqlPool r pconn = do
    mres <- withResourceTimeout 2000000 pconn $ runSqlConn r
    maybe (throwIO Couldn'tGetSQLConnection) return mres

runSqlConn :: MonadBaseControl IO m => SqlPersistT m a -> SqlBackend -> m a
runSqlConn r conn = do
    let getter = getStmtConn conn
    liftBase $ connBegin conn getter
    x <- onException
            (runReaderT r conn)
            (liftBase $ connRollback conn getter)
    liftBase $ connCommit conn getter
    return x

runSqlPersistM :: SqlPersistM a -> SqlBackend -> IO a
runSqlPersistM x conn = runSqlConn x conn

runSqlPersistMPool :: SqlPersistM a -> Pool SqlBackend -> IO a
runSqlPersistMPool x pool = runSqlPool x pool

withSqlPool :: (MonadIO m, MonadLogger m)
            => (LogFunc -> IO SqlBackend) -- ^ create a new connection
            -> Int -- ^ connection count
            -> (Pool SqlBackend -> m a)
            -> m a
withSqlPool mkConn connCount f = do
    pool <- createSqlPool mkConn connCount
    f pool

createSqlPool :: (MonadIO m, MonadLogger m)
              => (LogFunc -> IO SqlBackend)
              -> Int
              -> m (Pool SqlBackend)
createSqlPool mkConn size = do
    lf <- askLogFunc
    liftIO $ createPool (mkConn lf) close' 1 20 size

withSqlConn :: (MonadIO m, MonadBaseControl IO m, MonadLogger m)
            => (LogFunc -> IO SqlBackend)
            -> (SqlBackend -> m a)
            -> m a
withSqlConn open f = do
    lf <- askLogFunc
    bracket (liftIO $ open lf) (liftIO . close') f

close' :: SqlBackend -> IO ()
close' conn = do
    readIORef (connStmtMap conn) >>= mapM_ stmtFinalize . Map.elems
    connClose conn
