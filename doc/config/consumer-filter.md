
Full and usable samples can be found under [`test/config/`](test/config/)

#### Filter

- `sourcePath`: write a java class implements `MethodFilter`  to handle `SyncData`
  - Open a new maven project and import latest dependency (not import other dependency):
  ```xml
        <dependency>
            <groupId>com.github.zzt93</groupId>
            <artifactId>syncer-data</artifactId>
            <version>1.0.2-SNAPSHOT</version>
        </dependency>

  ```
  - Write a class implement `MethodFilter` **without package name**: use all api provided by `SyncData` to do any change you like
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
  - config it:
  ```yaml
  filter:
    sourcePath: /data/config/consumer/ColdFilter.java
  ```
  - debug code
    - add log if in production env by slf4j logger
    - [how to add breakpoint to debug?](https://github.com/zzt93/syncer/issues/18)


