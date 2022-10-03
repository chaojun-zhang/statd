package io.statd.core.storage.template;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TemplateUtilTest {

    @Test
    public void testSql() {
        String template = "select uid,sum(up) as up from  \"jarvis_charge_customer\" where ${timeRange} group by uid";
        Map<String, Object> model = new HashMap<>();
        model.put("filter", "domain='v4.kwaicdn.com'");
        String render = TemplateUtil.render(template, model);
        Assert.assertEquals(render, "select uid,sum(up) as up from  \"jarvis_charge_customer\" where domain='v4.kwaicdn.com' group by uid");
    }

}