{-# LANGUAGE OverloadedStrings #-}
import Data.Conduit.Kafka
import Haskakafka as HK
import Data.Conduit
import qualified Data.Conduit.Combinators as CC (print, mapM, mapM_, sinkNull, repeat)
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Concurrent (threadDelay, forkIO)
import System.Process

broker = "localhost:9092"
topic  = "test_topic"
kafkaConfig = [("socket.timeout.ms", "50000")]
topicConfig = [("request.timeout.ms", "50000")]
timeout = 5000

startKafka :: IO ()
startKafka = do
    createProcess . shell $ "docker run -d --net=host -e HOSTNAME=localhost --name=kafka-conduit-test tobegit3hub/standalone-kafka"
    threadDelay $ 10 * 1000 * 1000
    return ()

stopKafka :: IO ()
stopKafka = do
    createProcess . shell $ "docker rm -f kafka-conduit-test"
    return ()
 
readKafka :: IO ()  
readKafka = runResourceT $  kafkaSource broker topic 0 (HK.KafkaOffsetBeginning) timeout kafkaConfig topicConfig
                         $$ CC.print

writeKafka :: IO (Maybe KafkaError)
writeKafka = do
    runResourceT $  CC.repeat "hello world!"
                 $$ kafkaSink broker topic 0 kafkaConfig topicConfig

main :: IO ()
main = do
    startKafka
    forkIO $ readKafka
    forkIO $ (writeKafka >> return ())
    threadDelay $ 20 * 1000 * 1000
    stopKafka

    
