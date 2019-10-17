## 数据类型：

1. text      (json, xml, ...)
2. image  (jpg, png, ...)
3. audio  (wav, mp3, ...)
4. video  (avi, mp4, ...)
5. model?

## 应用场景：

#### 1. 图片基本处理(格式转换、旋转、...)
```sql 
CREATE TABLE `images` (`source` IMAGE NOT NULL, `target` IMAGE) 

INSERT INTO `images` (`source`) VALUES (image_file('file:///data/test.jpg'))

INSERT INTO `images` (`source`) SELECT image_file(`content`) FROM get_files('file:///data/images/')

UPDATE `images` SET `target` = (CASE image_format(`source`) = 'JPG' THEN `source` ELSE image_trans_format(`source`, 'JPG'))

SELECT image_trans_format(image_file('file:///data/test.png'), 'JPG')
```
#### 2. 模型训练&模型应用
```sql 
CREATE TABLE `images` (`source` IMAGE NOT NULL, `target` IMAGE)

CREATE TABLE `models` (`id` VARCHAR NOT NULL, `model` MODEL NOT NULL) PRIMARY KEY (`id`)

INSERT INTO `models` VALUES ('01', model_train_image_clustering(SELECT `target` FROM `images`, 'K-Means'))

INSERT INTO `models` VALUES ('02', model_file('file:///data/test.model'))

SELECT image_clustering(`model`, image_file('file:///data/test.jpg')) FROM (SELECT `model` FROM `models` WHERE `id` = '01')

SELECT get_cluster_label(`model`, get_cluster(`model`, image_file('file:///data/test.jpg')) FROM (SELECT `model` FROM `models` WHERE `id` = '01')
```
直接调用API进行推理的模式，抽象为API调用函数。

#### 3. 数据预标注
推理结果的格式与标注数据的格式是否天然一致？如果不一致，是否有固定的转换规则。

#### 4. 视频抽帧、大图切图
```sql 
CREATE TABLE `videos` (`source` VIDEO NOT NULL, `target` VIDEO)

CREATE TABLE `images` (`source` IMAGE NOT NULL, `target` IMAGE)

// 每隔1秒抽一帧，事实上，抽帧算法有很多，比如按画面的变化，抽帧函数的输入为视频，输出也为视频
UPDATE `videos` SET `target` = video_drawing_frame(`source`, 1000)

// 切图，起始像素坐标为(0, 0)，结束像素坐标为(100, 100)，坐标值可以考虑换成百分比
UPDATE `images` SET `target` = image_cut(`source`, 0, 0, 100, 100)
```

## 预置图片处理函数

1. image_cut(image, x0, y0, x1, y1)

2. image_rotate(image, angle)

3. image_resize(image, width, height)

4. image_add_noises(image)

5. image_trans_format(image, format)

6. get_image_width(image)

7. get_image_height(image)

8. get_image_format(image)

9. get_image_xxx(image)

## 预置视频处理函数

1. video_drawing_frame(video, interval)
2. video_trans_format(video, format)
3. get_video_format(video)
4. get_video_frame_rate(video)
5. get_video_xxx(video)