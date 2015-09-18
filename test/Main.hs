{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Concurrent (forkIO, threadDelay)
import Data.Conduit (($$), (=$=))
import qualified Data.Conduit.Binary as CB (lines)
import qualified Data.Conduit.Combinators as CC (stdin, stdout, print, repeat, mapM)
import Data.Conduit.Kafka
import Haskakafka as HK

inC  b t p co to = CC.stdin =$= CB.lines $$ kafkaSink b t p co to

outC b t p o ti co to = kafkaSource b t p o ti co to $$ CC.print

broker = "0.0.0.0"
topic  = "test_topic"
kafkaConfig = [("socket.timeout.ms", "50000")]
topicConfig = [("request.timeout.ms", "50000")]
timeout = -1 -- no timeout

main :: IO ()
main = do
    forkIO $ do
        runResourceT $ inC broker topic 0 kafkaConfig topicConfig
        return ()
    forkIO $ runResourceT $ outC broker topic 0 HK.KafkaOffsetBeginning timeout kafkaConfig topicConfig
    forkIO $ runResourceT $ outC broker topic 0 HK.KafkaOffsetBeginning timeout kafkaConfig topicConfig
    forkIO $ runResourceT $ outC broker topic 0 HK.KafkaOffsetBeginning timeout kafkaConfig topicConfig
    threadDelay $ 10 * 1000 * 1000
    runResourceT $ outC broker topic 0 HK.KafkaOffsetBeginning timeout kafkaConfig topicConfig
