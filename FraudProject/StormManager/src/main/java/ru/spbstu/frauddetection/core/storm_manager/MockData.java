package ru.spbstu.frauddetection.core.storm_manager;


import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.InputDataCalculator.ValueType;

import java.io.File;
import java.io.Serializable;
import java.util.*;

public class MockData extends AbstractData implements Serializable {
    
    public MockData() {
        super();
        initForSentenceCase();
    }
    
    public List<ValueGroup> getValues(List<Field> list) {
        List<ValueGroup> values = new ArrayList<ValueGroup>();
        
        for (ValueGroup group : super.getData()) {
            List<ValueType> valueList = new ArrayList<ValueType>();
            for (ValueType val : group.getValues()) {
                for (Field field : list) {
                    if (val.getFieldName().equals(field.getXpathName()))
                        valueList.add(val);
                }
            }
            if (!valueList.isEmpty()) {
                ValueGroup tmp = new ValueGroup();
                tmp.setValues(valueList);
                values.add(tmp);
            }
        }

        return values;
    }
    
    
    public void addValue(List<ValueType> values) {
        ValueGroup tmp = new ValueGroup();
        tmp.setValues(values);
        super.getData().add(tmp);
    }
    
    
    private void init() {
        
        ValueGroup valGroup = new ValueGroup();
        List<ValueType> vList = new ArrayList<ValueType>();
        ValueType v = new ValueType(20, "Potion");
        vList.add(v);
        v = new ValueType("Cold", "Desease");
        vList.add(v);
        v = new ValueType(3, "TimesPerDay");
        vList.add(v);
        v = new ValueType(26, "Age");
        vList.add(v);valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        v = new ValueType(5, "Potion");
        vList.add(v);
        v = new ValueType("Flu", "Desease");
        vList.add(v);
        v = new ValueType(3, "TimesPerDay");
        vList.add(v);
        v = new ValueType(6, "Age");
        vList.add(v);
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        v = new ValueType(100, "Potion");
        vList.add(v);
        v = new ValueType("Lupus", "Desease");
        vList.add(v);
        v = new ValueType(1, "TimesPerDay");
        vList.add(v);
        v = new ValueType(16, "Age");
        vList.add(v);
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        v = new ValueType(30, "Potion");
        vList.add(v);
        v = new ValueType("Cold", "Desease");
        vList.add(v);
        v = new ValueType(2, "TimesPerDay");
        vList.add(v);
        v = new ValueType(46, "Age");
        vList.add(v);
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        v = new ValueType(15, "Potion");
        vList.add(v);
        v = new ValueType("Cold", "Desease");
        vList.add(v);
        v = new ValueType(3, "TimesPerDay");
        vList.add(v);
        v = new ValueType(14, "Age");
        vList.add(v);
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
    }
    
    private void initForSentenceCase() {
        
        ValueGroup valGroup = new ValueGroup();
        List<ValueType> vList = new ArrayList<ValueType>();
        
        String s = "Больная очень своеобразно передала свои ощущения во время приступов удушья.\n" +
        "        Стало ясно, что речь идет о приступах бронхиальной астмы. Удушье, преимущественно ночные приступы,\n" +
        "        поведение больной при этом и, наконец, характер приступа с затруднением выдоха и хрипами в груди —\n" +
        "        все это является классической картиной бронхиальной астмы.\n" +
        "        О сердечной астме в данном случае речи быть не может. Никаких сердечных жалоб у\n" +
        "        больной нет, не говоря о ряде других моментов, отвергающих сердечное происхождение приступов\n" +
        "        (возраст, отсутствие декомпенсации и т. д.).\n" +
        "        Небольшая одышка, появившаяся недавно, может только свидетельствовать о некоторых изменениях\n" +
        "        в сердце именно теперь, после многих лет приступов. А это, вероятнее всего, проявление тех более стойких\n" +
        "        изменений в легких и бронхах, которые каждый раз остро возникают во время приступа.\n" +
        "        Если это бронхиальная астма, необходимо дополнительно выяснить ряд интересующих нас вопросов.\n" +
        "        Длится заболевание уже много лет, но больная не может вспомнить или указать, с чем бы она могла\n" +
        "        связать появление приступов — ни особых запахов, ни пищи,\n" +
        "        ни каких-либо нервных или психических раздражителей. За это время по совету врача переменила\n" +
        "        квартиру, но сколько-нибудь значительной разницы в состоянии здоровья не отмечает.";
        ValueType v = new ValueType(37, "age");
        vList.add(v);
        v = new ValueType(73, "weight");
        vList.add(v);
        v = new ValueType(132, "pressure");
        vList.add(v);
        v = new ValueType(150, "pulse");
        vList.add(v);
        v = new ValueType(s, "anamnesis");
        vList.add(v);
        
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        
        s = " На основании анамнеза можно предполагать, что еще в августе у больного было какое-то поражение листков плевры, так как всякий раз при дыхании он ощущал какой-то хруст с болью, что зависело, видимо, от трения листков плевры. Следовательно, надо допустить, что была неровная, негладкая поверхность, листков плевры, т. е. еще тогда у больного был сухой, или фибринозный плеврит.\n" +
        "        Повышения температуры в тот период не отмечал. Самочувствие было в общем удовлетворительное. Этиологическим моментом бывшего тогда плеврита, по-видимому, являлся туберкулез — наиболее частая причина плевритов вообще.\n" +
        "        Косвенным подтверждением действительно специфической этиологии плеврита является упоминание в анамнезе о диагностированном туберкулезе легких несколько лет тому назад при обследовании в одной из врачебных комиссий.\n" +
        "        В дальнейшем, но его словам, он перенес гриппозное заболевание, после которого резко усилились боли в боку, появился кашель и температура поднялась до 40°. Эта высокая температура совсем не характерна для сухого плеврита. Скорее следует допустить, что грипп осложнился каким-то добавочным процессом, так как температура затянулась на долгое время.\n" +
        "        Чаще всего грипп осложняется катаральной пневмонией. Ее можно было бы предположить и в данном случае. Но некоторые обстоятельства не позволяют нам окончательно укрепиться в этом мнении. Дело в том что больной был нездоров еще в августе.\n" +
        "        Есть основание думать, что вероятнее всего у него тогда был сухой плеврит. Собственно говоря, с тех пор он вполне здоровым себя не чувствовал. Следовательно, местом наименьшего сопротивления у больного оставалась именно плевра. Присоединившийся грипп, с нашей точки зрения, привел к осложнению либо со стороны легких, либо плевры, и наиболее логично представить себе, что это осложнение является обострением вяло текущего плеврита.";
        v = new ValueType(25, "age");
        vList.add(v);
        v = new ValueType(80, "weight");
        vList.add(v);
        v = new ValueType(169, "pressure");
        vList.add(v);
        v = new ValueType(110, "pulse");
        vList.add(v);
        v = new ValueType(s, "anamnesis");
        vList.add(v);
        
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        
        s = " Женщина, 33 лет, служащая, явилась на прием с жалобами на боль в правом подреберье. Боли постоянные, ноющие, то несколько усиливаются, то ослабевают. В течение нескольких лет больная никогда не чувствовала себя полностью здоровой. Всегда что-то мешает в правом подреберье.\n" +
        "        По ее словам, боли особенно усиливаются при погрешностях в диете, по еле грубой или острой пищи. Однажды она выпила немного алкоголя и потом всю ночь страдала от болей.\n" +
        "        Аппетит по временам хороший, но когда боли усиливаются, аппетит снижается или пропадает; временами появляется даже отвращение к еде. Она не исхудала, но в последнее время появилась слабость; настроение всегда угнетенное. Замужем; два раза благополучно рожала, последний раз около двух лег назад.\n" +
        "        Такой рассказ не представляет ничего определенного, и на основании его нельзя более или менее точно высказаться о природе заболевания. На первый взгляд все дело сводится к неприятным ощущениям после погрешностей в диете, особенно после грубой пищи, алкоголя и т. д.\n" +
        "        Наблюдается связь между обострением процесса и нарушением аппетита: аппетит то хороший, то снижается или даже пропадает совсем.\n" +
        "        Казалось бы, речь может идти о какой-то желудочной патологии. Однако дополнительный опрос показал, что боли не всегда связаны с приемом пищи. Так, они совсем не усиливаются каждый раз после еды, как это обычно бывает у желудочных больных. И только иногда усиление болей можно связать с погрешностями в диете, а в остальное время они ощущаются вне всякой зависимости от еды.";
        v = new ValueType(33, "age");
        vList.add(v);
        v = new ValueType(72, "weight");
        vList.add(v);
        v = new ValueType(120, "pressure");
        vList.add(v);
        v = new ValueType(101, "pulse");
        vList.add(v);
        v = new ValueType(s, "anamnesis");
        vList.add(v);
        
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        
        s = "Что касается гриппа, то заболевание это обычно длится два-четыре дня. Такое длительное течение, как у обследуемого больного, для неосложненного гриппа нехарактерно. Заболевание длится больше 10 дней, а температура остается повышенной.\n" +
        "        Колики в боку продолжаются, общее состояние стало довольно тяжелым.\n" +
        "        Высокая температура, колющая боль в груди, усиливающаяся при вдохе, напоминают начальные симптомы крупозной пневмонии или плеврита. Но кроме высокой температуры в течение 10 дней, других типичных для крупозной пневмонии данных нет Весьма важно то, что больной, почувствовав себя плохо, был в состоянии еще некоторое время продолжать работу.\n" +
        "        На следующий день снова вышел на работу, обращался в амбулаторию за помощью и, только совершенно ослабев, вынужден был слечь. Такое начало совсем не характерно для крупозной пневмонии, при которой с самого раннего периода больные «сваливаются» в постель. С другой стороны, отсутствие резких колик в боку и резких болей в груди при дыхании с первого же момента от начала заболевания делают маловероятным и заболевание плевры. Вся симптоматология, безусловно, характерна для какого-то острого инфекционного процесса, локализующегося в органах грудной клетки, скорее, бронхов. Об этом свидетельствует все усиливающийся приступообразный кашель.";
        v = new ValueType(30, "age");
        vList.add(v);
        v = new ValueType(89, "weight");
        vList.add(v);
        v = new ValueType(123, "pressure");
        vList.add(v);
        v = new ValueType(119, "pulse");
        vList.add(v);
        v = new ValueType(s, "anamnesis");
        vList.add(v);
        
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
        
        valGroup = new ValueGroup();
        vList = new ArrayList<ValueType>();
        
        s = "Следует сказать, что заболевания «органов правой половины живота» нередко сочетаются с заболеваниями желудка. Особенно это относится к червеобразному отростку. Эту комбинацию, довольно частую (по некоторым авторам, до 90%), вряд ли можно считать случайной.\n" +
        "        Вместе с тем трудно представить, чтобы аппендицит являлся, как думают некоторые, причиной заболевания желудка. Вероятнее всего, здесь существует связь, чисто конституциональная.\n" +
        "        Мы теперь хорошо знаем, что конституциональные особенности играют существенную роль в развитии желудочных заболеваний: люди, предрасположенные к желудочно-кишечным заболеваниям, страдают аппендицитом, колитом, гастритом, язвой и т. д. С другой стороны, согласно данным многих авторов (Н. Д. Стражеско и др.), речь может идти о рефлекторной связи этих органов.\n" +
        "        Какое же желудочное заболевание у данного больного. Долгий анамнез, отсутствие выраженного исхудания говорит против рака. Следовательно, остается допустить язву или гастрит. Более точно это можно решить после исследования больного.";
        v = new ValueType(37, "age");
        vList.add(v);
        v = new ValueType(90, "weight");
        vList.add(v);
        v = new ValueType(140, "pressure");
        vList.add(v);
        v = new ValueType(82, "pulse");
        vList.add(v);
        v = new ValueType(s, "anamnesis");
        vList.add(v);
        
        valGroup.setValues(vList);
        super.getData().add(valGroup);
        
    }
}
