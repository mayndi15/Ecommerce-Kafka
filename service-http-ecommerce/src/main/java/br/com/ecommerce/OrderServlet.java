package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class OrderServlet extends HttpServlet {

    private final KafkaProducers<Order> orderProducer = new KafkaProducers<>();
    private final KafkaProducers<String> emailProducer = new KafkaProducers<>();

    @Override
    public void destroy() {
        orderProducer.close();
        emailProducer.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = UUID.randomUUID().toString();

            var order = new Order(orderId, amount, email);
            orderProducer.send("ecommerce.new.order", email, order);

            var emailCode = "Thank you for your order! We are processing your order!";
            emailProducer.send("ecommerce.send.email", email, emailCode);

            System.out.println("New order sent successfully");

            resp.getWriter().println("New order sent!");
            resp.setStatus(HttpServletResponse.SC_OK);
        } catch (ExecutionException ex) {
            throw new ServletException(ex);
        } catch (InterruptedException ex) {
            throw new ServletException(ex);
        }
    }
}
