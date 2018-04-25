package ml.wonwoo.springtipredis;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.repository.CrudRepository;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.SessionAttributes;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
@EnableCaching
@EnableRedisHttpSession
public class SpringTipRedisApplication {


    private ApplicationRunner titledRunner(String title, ApplicationRunner rr) {
        return args -> {
            log.info(title.toUpperCase() + ":");
            rr.run(args);
        };
    }

    @Bean
    CacheManager redisCache(RedisConnectionFactory cf) {
        return RedisCacheManager
                .builder(cf)
                .build();
    }

    @Bean
    ApplicationRunner geography(RedisTemplate<String, String> rt) {
        return titledRunner("geography", args -> {
            GeoOperations<String, String> geo = rt.opsForGeo();
            geo.add("Sicily", new Point(13.361389, 38.1155556), "Arigento");
            geo.add("Sicily", new Point(15.087269, 37.502669), "Cataina");
            geo.add("Sicily", new Point(13.583333, 37.316667), "Palermo");

            Circle circle = new Circle(new Point(13.583333, 37.316667),
                    new Distance(100, RedisGeoCommands.DistanceUnit.KILOMETERS));

            GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults = geo.radius("Sicily", circle);

            geoResults
                    .getContent()
                    .forEach(c -> log.info(c.toString()));
        });
    }


    @Bean
    ApplicationRunner repositories(LineItemRepository lineItemRepository, OrderRepository orderRepository) {
        return titledRunner("repositories", args -> {
            Long orderId = generateId();
            List<LineItem> lineItems = Arrays.asList(new LineItem(orderId, generateId(), "plunger"),
                    new LineItem(orderId, generateId(), "soup"),
                    new LineItem(orderId, generateId(), "coffee mug"));

            lineItems
                    .stream()
                    .map(lineItemRepository::save)
                    .forEach(li -> log.info(li.toString()));

            Order order = new Order(orderId, new Date(), lineItems);
            orderRepository.save(order);
            Collection<Order> found = orderRepository.findByWhen(order.getWhen());
            found.forEach(o -> log.info("found:" + o.toString()));

        });
    }

    private final String topic = "chat";

    @Bean
    ApplicationRunner pubSub(RedisTemplate<String, String> rt) {
        return titledRunner("publish/subscribe", args -> {
            rt.convertAndSend(this.topic, "Hello, world !" + Instant.now().toString());
        });
    }

    @Bean
    RedisMessageListenerContainer listener(RedisConnectionFactory cf) {
        MessageListener ml = (message, pattern) -> {
            String str = new String(message.getBody());
            log.info("message from '" + this.topic + "': " + str);
        };
        RedisMessageListenerContainer mlc = new RedisMessageListenerContainer();
        mlc.addMessageListener(ml, new PatternTopic(this.topic));
        mlc.setConnectionFactory(cf);
        return mlc;
    }



    private Long generateId() {
        long tem = new Random().nextLong();
        return Math.max(tem, tem * -1);
    }


    //    public static class Cat {}
    //
    //    @Bean
    //    @ConditionalOnMissingBean(name = "redisTemplate")
    //    RedisTemplate<String, Cat> redisTemplate(
    //            RedisConnectionFactory redisConnectionFactory) {
    //        RedisTemplate<String, Cat> template = new RedisTemplate<>();
    //        RedisSerializer<Cat> values = new Jackson2JsonRedisSerializer<>(Cat.class);
    //        RedisSerializer<?> keys = new StringRedisSerializer();
    //        template.setConnectionFactory(redisConnectionFactory);
    //        template.setKeySerializer(keys);
    //        template.setValueSerializer(values);
    //        template.setHashValueSerializer(values);
    //        template.setHashKeySerializer(keys);
    //        return template;
    //    }


    private long measure(Runnable r) {
        long start = System.currentTimeMillis();
        r.run();
        long stop = System.currentTimeMillis();
        return stop - start;
    }

    @Bean
    ApplicationRunner cache(OrderService orderService) {
        return titledRunner("caching", args -> {
            Runnable measure = () -> orderService.byId(1L);
            log.info("first " + measure(measure));
            log.info("two " + measure(measure));
            log.info("three " + measure(measure));
        });
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringTipRedisApplication.class, args);
    }
}


@Service
class OrderService {

    @Cacheable("order-by-id")
    public Order byId(Long id) {
        //@formatter:off
        try {
            Thread.sleep(1000 * 10);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        //@formatter:on
        return new Order(id, new Date(), Collections.emptyList());
    }
}


interface OrderRepository extends CrudRepository<Order, Long> {

    Collection<Order> findByWhen(Date d);
}


interface LineItemRepository extends CrudRepository<LineItem, Long> {

}


@Data
@AllArgsConstructor
@NoArgsConstructor
@RedisHash("orders")
class Order implements Serializable {

    @Id
    private Long id;

    @Indexed
    private Date when;

    @Reference
    private List<LineItem> lineItems;
}


@RedisHash("lineItems")
@Data
@AllArgsConstructor
@NoArgsConstructor
class LineItem implements Serializable {

    @Indexed
    private Long orderId;

    @Id
    private Long id;

    private String description;
}


class ShoppingCart implements Serializable {

    private final Collection<Order> orders = new ArrayList<>();

    public void addOrder(Order order) {
        this.orders.add(order);
    }

    public Collection<Order> getOrders() {
        return orders;
    }
}


@Slf4j
@Controller
@SessionAttributes("cart")
class CartSessionController {

    private final AtomicLong ids = new AtomicLong();

    @ModelAttribute("cart")
    ShoppingCart cart() {
        log.info("creating new cart");
        return new ShoppingCart();
    }

    @GetMapping("/orders")
    String orders(@ModelAttribute("cart") ShoppingCart cart,
            Model model) {
        cart.addOrder(new Order(ids.incrementAndGet(), new Date(), Collections.emptyList()));
        model.addAttribute("orders", cart.getOrders());
        return "orders";
    }

}