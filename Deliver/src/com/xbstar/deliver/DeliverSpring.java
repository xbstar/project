package com.xbstar.deliver;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.xbstar.utils.PrintUtils;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.beans.Transient;
import java.io.IOException;
import java.util.Map;

public interface DeliverSpring
{
	@Bean //修改Jackson2ObjectMapper的配置来控制器生成JSON的行为
	default Jackson2ObjectMapperBuilderCustomizer deliverCustomizer()
	{
		return new Jackson2ObjectMapperBuilderCustomizer()
		{
			@Override
			public void customize(Jackson2ObjectMapperBuilder jacksonObjectMapperBuilder)
			{
				jacksonObjectMapperBuilder.modules(new SimpleModule().addSerializer(Deliver.class, new JsonSerializer<Deliver>()
				{
					@Override
					public void serialize(Deliver value, JsonGenerator gen, SerializerProvider serializers) throws IOException
					{
						gen.writeStartObject();
						Map<String, Object> store = value.toMap();
						for (String field : store.keySet()) gen.writeObjectField(field, store.get(field));
						gen.writeEndObject();
					}
				}).addDeserializer(Deliver.class, new JsonDeserializer()
				{
					@Override
					public Deliver deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException
					{
						ObjectCodec codec = jsonParser.getCodec();
						JsonNode node = codec.readTree(jsonParser);
						return Deliver.fromMap(codec.treeToValue(node, Map.class));
					}
				}));
			}
		};
	}

	default void interceptDeliver(InterceptorRegistry registry)
	{
		registry.addInterceptor(new HandlerInterceptor()
		{
			@Override
			public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception
			{
				// 判断是否拦截到Rest方法
				if (!(handler instanceof HandlerMethod) || "/error".equals(request.getServletPath())) return true;// 拦截的不是方法或者是Spring的报错方法不用处理
				// 获取到Spring的Controller方法
				HandlerMethod handlerMethod = (HandlerMethod) handler;
				// 通过反射判断是否标记了Transient注解
				Transient transientAnno = handlerMethod.getMethodAnnotation(Transient.class);
				if (transientAnno != null)
				{
					try
					{
						Class.forName("com.xbstar.deliver.ADBTEngine");
						ADBTEngine.getJDBCConnection().setAutoCommit(false);
						PrintUtils.printVerbose("[Deliver]开启事务提交");
					} catch (Exception e)
					{
						// 说明没有整合Deliver那就不用管了
					}
				}
				return true;
			}

			@Override
			public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception
			{
				// 判断是否拦截到Rest方法
				if (!(handler instanceof HandlerMethod) || "/error".equals(request.getServletPath())) return;// 拦截的不是方法或者是Spring的报错方法不用处理
				// 通过反射判断是否标记了Tran
				HandlerMethod handlerMethod = (HandlerMethod) handler;
				Transient transientAnno = handlerMethod.getMethodAnnotation(Transient.class);
				if (transientAnno != null)
				{
					try
					{
						Class.forName("com.xbstar.deliver.ADBTEngine");
						if (response.getStatus() == 200 || response.getStatus() == 201)
						{
							PrintUtils.printInfo("[Deliver]执行了事务提交");
							ADBTEngine.getJDBCConnection().commit();
						}
						else
						{
							PrintUtils.printWarning("[Deliver]执行了事务回滚");
							ADBTEngine.getJDBCConnection().rollback(); //出异常了要回滚
						}
						ADBTEngine.getJDBCConnection().setAutoCommit(true);
						PrintUtils.printVerbose("[Deliver]关闭事务提交");
					} catch (Exception e)
					{
						// 说明没有整合Deliver那就不用管了
					}
				}
			}
		}).addPathPatterns("/**");
	}
}
