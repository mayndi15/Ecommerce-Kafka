package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {

    private final KafkaProducers<String> batchProducer = new KafkaProducers<>();

    @Override
    public void destroy() {
        batchProducer.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            batchProducer.send("ecommerce.send.message.to.all.users", "ecommerce.user.generate.reading.report",
                    "ecommerce.user.generate.reading.report", new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()));

            System.out.println("Sent generate report all users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated!");

        } catch (ExecutionException ex) {
            throw new ServletException(ex);
        } catch (InterruptedException ex) {
            throw new ServletException(ex);
        }
    }
}
