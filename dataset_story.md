BİTİRME PROJESİ

Veri Seti: https://github.com/erkansirin78/datasets/raw/master/sensors_instrumented_in_an_office_building_dataset.zip

Veri Seti Kaynağı: https://www.kaggle.com/ranakrc/smart-building-system


Açıklama: Veri seti 
Bu veri seti, UC Berkeley'deki Sutardja Dai Salonu'nun (SDH) 4 katındaki 51 odada konumlanmış 255 sensör zaman serisinden toplanmıştır. Bir binadaki bir odanın fiziksel özelliklerindeki paternleri araştırmak için kullanılabilir. Ayrıca, Nesnelerin İnterneti (IoT), sensör füzyon ağı veya zaman serisi görevleriyle ilgili deneyler için de kullanılabilir. Bu veri seti hem denetimli (sınıflandırma ve regresyon) hem de denetimsiz öğrenme (kümeleme) görevleri için uygundur.

Her oda 5 tür ölçüm içerir: 
- CO2 konsantrasyonu, 
- humidity: oda hava nemi, 
- temperature: oda sıcaklığı, 
- light: parlaklık 
- PIR: hareket sensörü verileri

Veri 23 Ağustos 2013 Cuma ile 31 Ağustos 2013 Cumartesi arasında bir haftalık bir süre boyunca toplanmıştır. Hareket sensörü (PIR) her 10 saniyede bir değer üretir. Kalan sensörler her 5 saniyede bir değer üretir. Her dosya içinde zaman damgası ve sensör değerini içerir.

Pasif kızılötesi sensör (PIR sensörü), görüş alanındaki nesnelerden yayılan kızılötesi (IR) ışığı ölçen ve bir odadaki doluluğu ölçen elektronik bir sensördür. PIR verilerinin yaklaşık %6'sı sıfır değildir ve odanın doluluk durumunu gösterir. PIR verilerinin kalan %94'ü sıfırdır ve boş bir oda olduğunu gösterir.


Görevler:
Opsiyon-1:
.............
1. Bu veri setini kullanarak CO2, humidity, temperature, light ve zaman bilgileri bilinen bir odada herhangi bir aktivite veya hareketlilik olup olmadığını tahmin eden bir makine öğrenmesi modeli geliştiriniz.

2. Data-generator ile model geliştiriken kullandığınız test verisini, hedef değişkeni (pir) hariç tutarak Kafka office-input adında bir topic'e produce ediniz.

3. Spark streaming ile Kafka office-input topiğini consume ediniz. Modelinizi kullanarak hareketlilik bilgisini tahmin ediniz (odada aktivite var veya yok).

4. Odada hareketlilik var ise bunu office-activity topiğine, hareketlilik yok ise office-no-activity topiğine produce ediniz.

