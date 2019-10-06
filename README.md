## AISQL:  SQL for AI application


### 使用方式
```scala
val session = SparkSession.builder()
    .master("local")
    .withExtensions(new AISQLExtension)
    .getOrCreate()
session.sql("...")
```

### 功能介绍
#### 非结构化数据表格

```sql
-- 创建图片表格
CREATE TABLE image_table(
  path STRING,
  modificationTime TIMESTAMP,
  length LONG,
  image BINARY
) AS CARBONDATA;

-- 使用本地图片入库
INSERT INTO image_table 
SELECT * FROM binaryfile.`path/to/image`;
```


#### 使用Python UDF

可以注册python函数为UDF函数，对表格中的数据进行处理。例如在模型训练前对数据进行变换、预处理。
```sql
-- 将本地python脚本中的foo函数注册UDF函数
REGISTER PYFUNC foo INPATH 'my_script.py' AS my_udf;

--使用UDF函数
SELECT my_udf(image) FROM table1;
```

#### 使用内置的图片变换UDF

AISQL内置了20+种常用图片变换UDF，例如图片裁剪、旋转、加噪声、亮度增强等。
```sql
-- 调整图片大小为256*256
INSERT INTO result_table
SELECT RESIZE(image, 256, 256) FROM image_table;
```


#### 运行任意Python脚本

有时希望执行任意python脚本，而不仅仅对每一行数据进行处理，可以使用如下语法。在分布式环境中，此python脚本会在一个随机挑选的worker中执行一遍。
```sql
RUN SCRIPT 'path/to/my_script.py' 
PYFUNC foo 
WITH PARAMS ('x'='4')
OUTPUT (value INT);
```


#### 使用OCR API

AISQL内置了9个UDF，调用华为云OCR API从图片中提取结构化信息：
- 身份证识别：ocr_id_card
- 驾驶证识别：ocr_driver_license
- 行驶证识别：ocr_vehicle_license
- 增值税发票识别：ocr_vat_invoice
- 英文海关单据识别：ocr_form
- 通用表格识别：ocr_general_table
- 通用文字识别：ocr_general_text
- 手写数字识别：ocr_handwriting
- 机动车销售发票识别：ocr_mvs_invoice

使用场景：
1. 在数据准备阶段，可用于对图片打标签。
2. 可以作为对图片的批量推理。

```sql
-- 调用OCR API识别图片
INSERT INTO training_table
SELECT path, ocr_id_card(image) 
FROM image_table;
```


#### 使用本地TensorFlow模型做推理

使用已有的TensorFlow模型（或使用AISQL训练输出的模型），在Worker内完成推理，无需远程调用TensorFlow Serving服务，提升批量推理性能，减少对推理服务的压力。
```sql
-- 将本地的TF模型注册为UDF
REGISTER LOCAL MODEL INPATH 'path/to/model' AS my_udf 
OPTIONS ('outut_operation'='resnet_v1_50/SpatialSqueeze:0', ...');

-- 使用UDF做批量推理
SELECT my_udf(image) from image_table;
```


#### 使用华为云ModelArts服务做模型训练和推理

AISQL内置了ModelArts服务API，可以通过SQL调用模型训练、模型管理、部署在线推理服务、调用在线推理服务。
在模型训练的python脚本中，使用[pycarbon](http://github.com/carbonlake/pycarbon)读取表格数据对接TensorFlow/Pytorch/Mxnet等AI框架。
```sql
-- 创建训练模板
CREATE EXPERIMENT flower_exp
OPTIONS('dataset_name'='flower', ...);

-- 调用ModelArts训练模型
CREATE MODEL model1 
USING EXPERIMENT flower_exp
OPTIONS('train_url'='path/to/flower_model/');

-- 将模型注册为UDF
REGISTER MODEL flower_exp.model1 AS flower_udf;

-- 使用UDF做批量推理
SELECT flower_udf(image) FROM image_table;

-- 显示模型信息
SELECT * FROM MODEL_INFO(flower_udf);
```

页面访问次数: [![HitCount](http://hits.dwyl.io/jackylk/carbonlake/aisql.svg)](http://hits.dwyl.io/jackylk/carbonlake/aisql)
