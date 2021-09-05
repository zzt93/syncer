完整和可用的示例可以在 [`test/config/`](../../test/config/) 下找到

#### Filter

- `sourcePath`：编写一个java类实现`MethodFilter`来处理`SyncData`
  - 打开一个新的 maven 项目并导入最新的依赖项（不导入其他依赖项）：
  ```xml
        <dependency>
            <groupId>com.github.zzt93</groupId>
            <artifactId>syncer-data</artifactId>
            <version>1.0.2-SNAPSHOT</version>
        </dependency>

  ```
  - 编写一个类实现`MethodFilter`（注意**不能有包名**）：使用`SyncData`提供的所有api来做你需要的任何改变
  ```java
    import com.github.zzt93.syncer.data.SyncData;
    import com.github.zzt93.syncer.data.util.MethodFilter;
    import com.github.zzt93.syncer.data.util.SyncUtil;
    
    import java.util.List;
    
    /**
     * @author zzt
     */
    public class Simplest implements MethodFilter {
      @Override
      public void filter(List<SyncData> list) {
        SyncData sync = list.get(0);
        SyncUtil.unsignedByte(sync, "tinyint");
        SyncUtil.unsignedByte(sync, "type");
      }
    }

  ```
  - 配置它：
  ```yaml
  filter:
    sourcePath: /data/config/consumer/ColdFilter.java
  ```
  - 调试代码
    - 通过 slf4j的logger添加日志
    - [如何添加断点调试？](https://github.com/zzt93/syncer/issues/18)