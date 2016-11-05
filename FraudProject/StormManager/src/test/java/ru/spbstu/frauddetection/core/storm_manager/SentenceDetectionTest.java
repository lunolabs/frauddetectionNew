package ru.spbstu.frauddetection.core.storm_manager;

import org.junit.Test;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.InputDataCalculator.ValueType;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SentenceDetectionTest {
    private final String configStr =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                "<fraudconfig>\n" +
                "    <group method=\"Sentence\">\n" +
                "        <field>\n" +
                "            <xpath_name>Text</xpath_name>\n" +
                "            <type>String</type>\n" +
                "        </field>\n" +
                "    </group>\n" +
                "</fraudconfig>";

    @Test
    public void testFraudDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);
        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType("У людей, страдающих гипертонией, давление может резко и сильно вырасти до " +
                "критических значений, угрожающих здоровью.", "Text");
        ValueGroup valueGroup = createNewValueGroup(value);

        fraudDetectable.put(Method.Sentence, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertTrue(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    @Test
    public void testFraudNotDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);
        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType("Кредитный рейтинг России в 2016 году достигнет рекордных величин.", "Text");
        ValueGroup valueGroup = createNewValueGroup(value);

        fraudDetectable.put(Method.Sentence, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertFalse(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    private List getTestDBList() {
        List<ValueGroup> dbList = new ArrayList<>();

        String text = "Объемы добычи нефти составили за первое полугодие 270 миллионов тонн – это на 2,1% выше по отношению к прошлому полугодию, рассказал министр энергетики России Александр Новак на встрече с президентом Владимиром Путиным.\n" +
                "Глава государства провел рабочую встречу с министром, на которой обсуждалась ситуация в топливно-энергетическом комплексе, сообщили Накануне.RU в пресс-службе Кремля.\n" +
                "Российские компании гонят на экспорт сырую нефть.\n" +
                "Переработка снижается\n" +
                "Кабмин утвердил стратегию развития нефтехима\n" +
                "Объемы  добычи угля мы выросли примерно на 6% до 186 млн т.\n" +
                "В газовой отрасли произошло снижение добычи на 1,3%.\n" +
                "\"Это связано было с большей загрузкой гидроэлектростанций, а также с более тёплой зимой.\n" +
                "К концу года мы планируем выйти примерно на те же показатели, что и в прошлом году\", - пояснил министр.\n" +
                "Новак напомнил, что с 1 июля все российские предприятия перешли на потребление нефтепродуктов пятого класса.\n" +
                "За счет модернизации и НПЗ и ввода 12 установок предприятия смогли исполнить требования к экологическому классу топлива.\n" +
                "\"В прошлом году ввиду финансовых трудностей 16 установок были перенесены на более поздние сроки.\n" +
                "Мы в этом году введем 12 установок и в период включительно по 2018 г.\n" +
                "еще 36 установок\", - сказал Новак, добавив, что за счет модернизации снизилось производство мазута.";

        String[] sentences = text.split("\n");

        for (int i = 0; i < sentences.length; ++i) {
            ValueType v = new ValueType(sentences[i], "Text");
            dbList.add(createNewValueGroup(v));
        }

        return dbList;
    }


    private ValueGroup createNewValueGroup(ValueType... values) {
            ValueGroup group = new ValueGroup();
            group.setValues(Arrays.asList(values));

            return group;
    }
}
