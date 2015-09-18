{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE StandaloneDeriving #-}
module Data.Conduit.Kafka where

import Control.Lens
import Data.Default (Default, def)
import Data.ByteString as BS (ByteString)
import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Resource (MonadResource)
import Data.Conduit ( Producer(..)
                    , Consumer(..)
                    , bracketP
                    , yield
                    , await
                    ) 
import Haskakafka ( Kafka
                  , KafkaError (..)
                  , KafkaMessage (..)
                  , KafkaProduceMessage (..)
                  , KafkaProducePartition (..)
                  , KafkaTopic
                  , KafkaOffset(..)
                  , newKafka
                  , newKafkaTopic
                  , addBrokers
                  , startConsuming
                  , stopConsuming
                  , consumeMessage
                  , produceMessage
                  , drainOutQueue
                  )
import Haskakafka.InternalRdKafkaEnum ( RdKafkaTypeT(..)
                                      , RdKafkaRespErrT(..)
                                      )
deriving instance Show KafkaOffset
deriving instance Eq KafkaOffset
data KafkaSettings = KafkaSettings { _brokers         :: !String
                                   , _topic           :: !String
                                   , _partition       :: !Int
                                   , _offset          :: !KafkaOffset
                                   , _timeout         :: !Int
                                   , _configOverrides :: [(String, String)]
                                   , _topicOverrides  :: [(String, String)]
                                   } deriving (Show, Eq)
makeLenses ''KafkaSettings 
instance Default KafkaSettings where
    def = KafkaSettings { _brokers   = "0.0.0.0"
                        , _topic     = "default_topic"
                        , _partition = 0
                        , _offset    = KafkaOffsetBeginning
                        , _timeout   = -1 -- no timeout
                        , _configOverrides = [("socket.timeout.ms", "50000")]
                        , _topicOverrides  = [("request.timeout.ms", "50000")]
                        }

kafkaSource :: forall m. (MonadResource m, MonadIO m) => KafkaSettings -> Producer m KafkaMessage
kafkaSource ks = bracketP init fini consume
    where
        init :: IO (Kafka, KafkaTopic)
        init = do 
            kafka <- newKafka RdKafkaConsumer (ks^.configOverrides)
            addBrokers kafka (ks^.brokers)
            topic <- newKafkaTopic kafka (ks^.topic) (ks^.topicOverrides)
            startConsuming topic (ks^.partition) (ks^.offset)
            return (kafka, topic)

        fini :: (Kafka, KafkaTopic) -> IO ()
        fini (_kafka, topic) = liftIO $ stopConsuming topic (ks^.partition)

        consume :: (MonadIO m) => (Kafka, KafkaTopic) -> Producer m KafkaMessage
        consume (k, t) = do
            r <- liftIO $ consumeMessage t (ks^.partition) (ks^.timeout)
            case r of
                Left _error -> do
                    case _error of
                        KafkaResponseError RdKafkaRespErrPartitionEof -> do
                            liftIO $ threadDelay $ 1000 * 1000
                            consume(k, t)
                        otherwise -> do
                            liftIO $ print . show $  _error
                            return ()
                Right m -> do
                    yield m
                    consume (k, t)


kafkaSink :: (MonadResource m, MonadIO m) => KafkaSettings -> Consumer BS.ByteString m (Maybe KafkaError)
kafkaSink ks = bracketP init fini produce
    where
        init :: IO (Kafka, KafkaTopic)
        init = do 
            kafka <- newKafka RdKafkaProducer (ks^.configOverrides)
            addBrokers kafka (ks^.brokers)
            topic <- newKafkaTopic kafka (ks^.topic) (ks^.topicOverrides)
            return (kafka, topic)
       
        fini :: (Kafka, KafkaTopic) -> IO () 
        fini (_kafka, _) = liftIO $ drainOutQueue _kafka

        produce :: (MonadIO m) => (Kafka, KafkaTopic) -> Consumer BS.ByteString m (Maybe KafkaError)
        produce (_, _topic) = forever $ do
            _msg <- await
            case _msg of
                Just msg -> liftIO $ produceMessage _topic (KafkaSpecifiedPartition (ks^.partition)) (KafkaProduceMessage msg)
                Nothing -> return $ Just (KafkaError "empty stream")

