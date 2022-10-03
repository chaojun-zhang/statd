package io.statd.core.storage.template;


import io.statd.core.exception.StorageConfigException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Map;

@Slf4j
public class TemplateUtil {

    public static String render(String template, Map<String, Object> model) {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
        // Recommended settings for new projects:
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        cfg.setWrapUncheckedExceptions(true);
        cfg.setFallbackOnNullLoopVariable(false);
        try {
            StringReader stringReader = new StringReader(template);
            Template t = new Template("template", stringReader, cfg);
            StringWriter printWriter = new StringWriter();
            t.process(model, printWriter);
            String result = printWriter.toString();
            printWriter.close();
            stringReader.close();
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TemplateException e) {
            throw new StorageConfigException(String.format("fail to render template %s", template), e);
        }

    }

}
