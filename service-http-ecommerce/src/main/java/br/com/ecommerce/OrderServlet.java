package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class OrderServlet extends HttpServlet {

    private final KafkaProducers<Order> orderProducer = new KafkaProducers<>();

    @Override
    public void destroy() {
        orderProducer.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var orderId = req.getParameter("uuid");
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);

            try (var database = new OrderDataBase()) {

                if (database.saveNew(order)) {
                    orderProducer.send("ecommerce.new.order", email, order, new CorrelationId(OrderServlet.class.getSimpleName()));

                    System.out.println("New order sent successfully");
                    resp.getWriter().println("New order sent!");
                    resp.setStatus(HttpServletResponse.SC_OK);
                } else {
                    System.out.println("Old order received");
                    resp.getWriter().println("Old order received!");
                    resp.setStatus(HttpServletResponse.SC_OK);
                }
            }
        } catch (ExecutionException | SQLException | InterruptedException ex) {
            throw new ServletException(ex);
        }
    }
}
