# frozen_string_literal: true

describe Kafka::Cluster do
  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:broker_pool) { double(:broker_pool) }

    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: [URI("kafka://test1:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )
    }

    before do
      allow(broker_pool).to receive(:connect) { broker }
      allow(broker).to receive(:disconnect)
    end

    it 'raises NotLeaderForPartition if the current broker is not the leader' do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            partitions: [
              Kafka::Protocol::MetadataResponse::PartitionMetadata.new(
                partition_id: 42,
                leader: 2,

                partition_error_code: 6, # <-- this is the important bit. NotLeaderForPartition
              )
            ]
          )
        ]
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::NotLeaderForPartition
    end

    it "raises LeaderNotAvailable if there's no leader for the partition" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            partitions: [
              Kafka::Protocol::MetadataResponse::PartitionMetadata.new(
                partition_id: 42,
                leader: 2,
                partition_error_code: 5, # <-- this is the important bit.
              )
            ]
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::LeaderNotAvailable
    end

    it "raises InvalidTopic if the topic is invalid" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::Protocol::MetadataResponse::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            topic_error_code: 17, # <-- this is the important bit.
            partitions: []
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::InvalidTopic
    end

    it "raises ConnectionError if unable to connect to any of the seed brokers" do
      cluster = Kafka::Cluster.new(
        seed_brokers: [URI("kafka://not-there:9092"), URI("kafka://not-here:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )

      allow(broker_pool).to receive(:connect).and_raise(Kafka::ConnectionError)

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_exception(Kafka::ConnectionError)
    end

    context 'meta data is stale' do
      before do
        allow(cluster).to receive(:stale) { true }
      end

      it 'should re fetch metadata' do
        metadata = Kafka::Protocol::MetadataResponse.new(
          brokers: [
            Kafka::Protocol::MetadataResponse::BrokerInfo.new(
              node_id: 42,
              host: "test1",
              port: 9092,
            )
          ],
          controller_id: 42,
          topics: [
            Kafka::Protocol::MetadataResponse::TopicMetadata.new(
              topic_name: "greetings",
              partitions: [
                Kafka::Protocol::MetadataResponse::PartitionMetadata.new(
                  partition_id: 42,
                  leader: 42,
                  partition_error_code: 0
                )
              ]
            )
          ]
        )

        allow(broker).to receive(:fetch_metadata) { metadata }
        expect(cluster).to receive(:refresh_metadata!)

        cluster.get_leader("greetings", 42)
      end
    end
  end
end
