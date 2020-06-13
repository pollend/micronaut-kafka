package io.micronaut.configuration.kafka.annotation


import io.micronaut.context.ApplicationContext
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.RecordMetadata
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.ToxiproxyContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

class KafkaSendToRetrySpec extends Specification {

    @Shared
    @AutoCleanup
    public Network network = Network.newNetwork();

    @Shared
    @AutoCleanup
    KafkaContainer kafkaContainer = new KafkaContainer().withNetwork(network);
    @Shared
    @AutoCleanup
    KafkaContainer kafkaContainer1 = new KafkaContainer().withNetwork(network);
    @Shared
    @AutoCleanup
    ToxiproxyContainer toxiproxy = new ToxiproxyContainer()
        .withNetwork(network);

    @Shared
    @AutoCleanup
    ApplicationContext context

    @Shared
    public static ToxiproxyContainer.ContainerProxy proxy


    def setupSpec() {
        kafkaContainer.start()
        kafkaContainer1.start()
        toxiproxy.start()
        proxy = toxiproxy.getProxy(kafkaContainer,KafkaContainer.KAFKA_PORT)

        createTopics(kafkaContainer, ["KafkaSendToRetrySpec-pass", "KafkaSendToRetrySpec-products-receive-1"])
        createTopics(kafkaContainer1, ["KafkaSendToRetrySpec-products-receive-2"])

        context = ApplicationContext.run([
            "kafka.bootstrap.servers": [ "PLAINTEXT://"+ proxy.getContainerIpAddress() + ":" + proxy.getProxyPort()]
        ])

    }


    private static void createTopics(KafkaContainer container, List<String> topics) {
        def newTopics = topics.collect { topic -> new NewTopic(topic, 1, (short) 1) }
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers());
        def admin = AdminClient.create(properties)
        CreateTopicsResult createTopicsResult = admin.createTopics(newTopics)
        createTopicsResult.all().get()
    }

    void "test send and receive"() {
        given:
        ProductClient client = context.getBean(ProductClient.class)
        ProductListener listener = context.getBean(ProductListener.class)
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        listener.products.clear()
        listener.receive.clear()

        when:
        client.sendProduct("Apple", new Product(name: "Apple", quantity: 5))

        then:
        conditions.eventually {
            listener.products.size() == 1
        }

    }

    @KafkaClient(acks = KafkaClient.Acknowledge.ALL)
    static interface ProductClient {
        @Topic(value = "KafkaSendToRetrySpec-pass")
        RecordMetadata sendProduct(@KafkaKey String name, Product product);

    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST, redelivery = true)
    static class ProductListener {
        Queue<Product> products = new ConcurrentLinkedDeque<>()
        Queue<Product> receive = new ConcurrentLinkedDeque<>()

        @Topic(value = "KafkaSendToRetrySpec-pass")
        @SendTo(["KafkaSendToRetrySpec-products-receive-1", "KafkaSendToRetrySpec-products-receive-2"])
        Product receive(Product product) {
            products.add(product)
            return product
        }

        @Topic("KafkaSendToRetrySpec-products-receive-1")
        void receive1(Product message) {
            receive.add(message)
        }

        @Topic("KafkaSendToRetrySpec-products-receive-2")
        void receive2(Product message) {
            receive.add(message)
        }
    }


    static class Product {
        String name
        int quantity
    }
}
