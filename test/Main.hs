{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Lens
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Concurrent (forkIO, threadDelay)
import Data.Conduit (($$), (=$=))
import Data.Default (def)
import qualified Data.Conduit.Binary as CB (lines)
import qualified Data.Conduit.Combinators as CC (stdin, stdout, print, repeat, mapM)
import Data.Conduit.Kafka
import Haskakafka as HK

inC  ks = CC.stdin =$= CB.lines $$ kafkaSink ks
outC ks = kafkaSource ks $$ CC.print

kafkaSettings = def -- & configOverrides .~ [("socket.timeout.ms", "50000")]
                    -- & topicOverrides  .~ [("request.timeout.ms", "50000")]


main :: IO ()
main = do
    forkIO $ runResourceT $ inC kafkaSettings >> return ()
    forkIO $ runResourceT $ outC kafkaSettings
    forkIO $ runResourceT $ outC kafkaSettings
    forkIO $ runResourceT $ outC kafkaSettings
    threadDelay $ 10 * 1000 * 1000
    runResourceT $ outC kafkaSettings
