{-# LANGUAGE Rank2Types #-}
module Data.Conduit.Kafka (
      kafkaSource
    , kafkaSink
    ) where

import Data.ByteString as BS
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
                  , KafkaOffset
                  , newKafka
                  , newKafkaTopic
                  , addBrokers
                  , startConsuming
                  , stopConsuming
                  , consumeMessage
                  , produceMessage
                  , drainOutQueue
                  )
import Haskakafka.InternalRdKafkaEnum (RdKafkaTypeT(..))

kafkaSource :: forall m. (MonadResource m, MonadIO m) 
            => String             -- broker
            -> String             -- topic
            -> Int                -- partition
            -> KafkaOffset        -- offset
            -> Int                -- timeout
            -> [(String, String)] -- config overrides
            -> [(String, String)] -- topic overrides
            -> Producer m KafkaMessage
kafkaSource broker 
            topic 
            partition 
            offset 
            timeout
            config_overrides
            topic_overrides
            = bracketP init fini consume
    where
        init :: IO (Kafka, KafkaTopic)
        init = do 
            kafka <- newKafka RdKafkaConsumer config_overrides
            addBrokers kafka broker
            topic <- newKafkaTopic kafka topic topic_overrides
            startConsuming topic partition offset
            return (kafka, topic)

        fini :: (Kafka, KafkaTopic) -> IO ()
        fini (_kafka, topic) = liftIO $ stopConsuming topic partition

        consume :: (MonadIO m) => (Kafka, KafkaTopic) -> Producer m KafkaMessage
        consume (k, t) = do
            r <- liftIO $ consumeMessage t partition timeout
            case r of
                Left _error -> return ()
                Right m -> do
                    yield m
                    consume (k, t)


kafkaSink :: (MonadResource m, MonadIO m)
          => String -- broker
          -> String -- topic
          -> Int    -- partition
          -> [(String, String)] -- config overrides
          -> [(String, String)] -- topic overrides
          -> Consumer BS.ByteString m (Maybe KafkaError)
kafkaSink broker 
          topic 
          partition 
          config_overrides
          topic_overrides = bracketP init fini produce
    where
        init :: IO (Kafka, KafkaTopic)
        init = do 
            kafka <- newKafka RdKafkaProducer config_overrides
            addBrokers kafka broker
            topic <- newKafkaTopic kafka topic topic_overrides
            return (kafka, topic)
       
        fini :: (Kafka, KafkaTopic) -> IO () 
        fini (_kafka, _) = liftIO $ drainOutQueue _kafka

        produce :: (MonadIO m) => (Kafka, KafkaTopic) -> Consumer BS.ByteString m (Maybe KafkaError)
        produce (_, _topic) = forever $ do
            _msg <- await
            case _msg of
                Just msg -> liftIO $ produceMessage _topic (KafkaSpecifiedPartition partition) (KafkaProduceMessage msg)
                Nothing -> return $ Just (KafkaError "empty stream")

