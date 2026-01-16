package com.xbstar.deliver;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

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
}
