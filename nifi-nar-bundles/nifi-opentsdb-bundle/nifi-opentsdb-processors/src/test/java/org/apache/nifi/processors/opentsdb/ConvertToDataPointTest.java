package org.apache.nifi.processors.opentsdb;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Description:
 *
 * @author bright
 */
public class ConvertToDataPointTest {

    @Test
    public void test1() {
        String[] names = {"2013-11-01_2013-12-01PF_F035_5s.csv", "2013-11-01_2013-12-01PF_F035.csv"};

        Arrays.stream(names).forEach(name -> {
            System.out.println(name.replace(".csv", "").replace("_5s", "").substring(name.indexOf("PF_F")));
        });
    }


    @Test
    public void test2() {
        String json = "[\n" +
                "    {\"field\": \"WT_ID\", \"metric\": \"WT_ID\"},\n" +
                "    {\"field\": \"Turbine_Status\", \"metric\": \"Turbine_Status\"},\n" +
                "    {\"field\": \"Gen_Speed\", \"metric\": \"Gen_Speed\"},\n" +
                "    {\"field\": \"Torque\", \"metric\": \"Torque\"},\n" +
                "    {\"field\": \"Tower_Acc\", \"metric\": \"Tower_Acc\"},\n" +
                "    {\"field\": \"Drive_Train_Acc\", \"metric\": \"Drive_Train_Acc\"},\n" +
                "    {\"field\": \"GB_Oil_Temp\", \"metric\": \"GB_Oil_Temp\"},\n" +
                "    {\"field\": \"GB_Shaft_Temp_A\", \"metric\": \"GB_Shaft_Temp_A\"},\n" +
                "    {\"field\": \"GB_Shaft_Temp_B\", \"metric\": \"GB_Shaft_Temp_B\"},\n" +
                "    {\"field\": \"Shaft_Bearing_Temp\", \"metric\": \"Shaft_Bearing_Temp\"},\n" +
                "    {\"field\": \"External_Heater_Temp\", \"metric\": \"External_Heater_Temp\"},\n" +
                "    {\"field\": \"Ambient_Temp\", \"metric\": \"Ambient_Temp\"},\n" +
                "    {\"field\": \"Blade1_Angle1\", \"metric\": \"Blade1_Angle1\"},\n" +
                "    {\"field\": \"Blade2_Angle1\", \"metric\": \"Blade2_Angle1\"},\n" +
                "    {\"field\": \"Blade3_Angle1\", \"metric\": \"Blade3_Angle1\"},\n" +
                "    {\"field\": \"Blade1_Angle2\", \"metric\": \"Blade1_Angle2\"},\n" +
                "    {\"field\": \"Blade2_Angle2\", \"metric\": \"Blade2_Angle2\"},\n" +
                "    {\"field\": \"Blade3_Angle2\", \"metric\": \"Blade3_Angle2\"},\n" +
                "    {\"field\": \"Nacelle_Position\", \"metric\": \"Nacelle_Position\"},\n" +
                "    {\"field\": \"Nacelle_Temp\", \"metric\": \"Nacelle_Temp\"},\n" +
                "    {\"field\": \"Power\", \"metric\": \"Power\"},\n" +
                "    {\"field\": \"Wind_Speed\", \"metric\": \"Wind_Speed\"}\n" +
                "  ]";
        List<Rule> list = JSONArray.parseArray(json, Rule.class);
        list.forEach(r -> {
            System.out.println(JSONObject.toJSON(r));
        });
    }
}
