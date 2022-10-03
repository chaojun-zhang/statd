package io.statd.server.metric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import freemarker.template.DefaultListAdapter;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;

import java.io.UncheckedIOException;
import java.util.List;

public class TablePrinter implements TemplateMethodModelEx {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Object exec(List args) throws TemplateModelException {
        if (args.size() < 1 || args.size() > 2) {
            throw new TemplateModelException("Wrong arguments");
        }
        DefaultListAdapter adapter = (DefaultListAdapter) args.get(0);
        List records = (List) adapter.getAdaptedObject(List.class);
        boolean summarize = false;
        if (args.size() == 2) {
            summarize = "true".equalsIgnoreCase(args.get(1).toString()) && records.size() == 1;
        }
        try {
            if (summarize) {
                return objectMapper.writeValueAsString(records.get(0));
            } else {
                return objectMapper.writeValueAsString(records);
            }
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }

    }
}
