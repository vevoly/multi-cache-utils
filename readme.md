# J-MultiCache

一个轻量级、无侵入、支持多级缓存（L1-Caffeine, L2-Redis）和多种缓存策略的Java工具库。

## 特性

- **多级缓存**：自动处理 L1 (Caffeine 本地缓存) 和 L2 (Redis 分布式缓存) 的读取和写入。
- **缓存穿透保护**：内置分布式锁和空值缓存机制。
- **多种数据结构**：通过策略模式，支持对 String, List, Set, Page 等多种数据结构的缓存。
- **灵活的缓存策略**：支持 `L1_L2_DB`, `L1_DB`, `L2_DB` 等多种缓存策略，满足不同场景的需求。
- **Spring Boot友好**：提供自动配置，开箱即用。

## 快速开始

### 1. 添加依赖

将以下依赖添加到您的 `pom.xml` 中：
```xml
<dependency>
    <groupId>com.github.yourusername</groupId> <!-- 将发布到你自己的GitHub Packages -->
    <artifactId>j-multicache</artifactId>
    <version>1.0.0</version>
</dependency>
<!-- 确保您的项目中已有以下依赖 -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
<dependency>
    <groupId>org.redisson</groupId>
    <artifactId>redisson-spring-boot-starter</artifactId>
</dependency>
<dependency>
    <groupId>com.github.ben-manes.caffeine</groupId>
    <artifactId>caffeine</artifactId>
</dependency>