package com.xbstar.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.TypeMismatchException;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcRegistrations;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.ConfigurableWebBindingInitializer;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.annotation.RequestParamMethodArgumentResolver;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.support.DefaultHandlerExceptionResolver;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.beans.PropertyChangeEvent;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Configuration
@EnableWebSocket
@RestControllerAdvice
@SpringBootApplication
public abstract class XBStarSpring implements WebMvcConfigurer, WebMvcRegistrations, ApplicationContextAware, WebSocketConfigurer
{
	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationReady() throws Exception
	{
	}

	@Override
	public void registerWebSocketHandlers(WebSocketHandlerRegistry registry)
	{
	}

	public boolean onAuthoriseRequest(HttpServletRequest request)
	{
		return true;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException
	{
		try
		{
			//使用反射注入ApplicationContext
			Field field = ContextLoader.class.getDeclaredField("currentContext");
			field.setAccessible(true);
			field.set(null, applicationContext);
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override // 配置SpringMVC的请求拦截器
	public void addInterceptors(InterceptorRegistry registry)
	{
		// 配置Deliver拦截器
		try
		{
			Class<?> deliverSpring = Class.forName("com.xbstar.deliver.DeliverSpring");
			if (deliverSpring.isInstance(this))
			{
				Method method = deliverSpring.getMethod("interceptDeliver", InterceptorRegistry.class);
				method.invoke(this, registry);
			}
		} catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		// 配置全局拦截器
		registry.addInterceptor(new HandlerInterceptor()
		{
			@Override
			public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception
			{
				// 所有接口返回Header无条件补充sprint-url参数
				response.setHeader("spring-url", request.getRequestURL().toString());
				// 判断是否拦截到Rest方法
				if (!(handler instanceof HandlerMethod) || "/error".equals(request.getServletPath())) return true;// 拦截的不是方法或者是Spring的报错方法不用处理
				// 获取到Spring的Controller方法
				HandlerMethod handlerMethod = (HandlerMethod) handler;
				String print = ContextLoader.getCurrentWebApplicationContext().getEnvironment().getProperty("logging.xbstar");
				if ("true".equals(print)) System.out.println(request.getRequestURL()); //打印蓝色的地址出来
				// 通过反射判断是否标记了XBStarAuthorise注解
				XBStarAuthorise autoAnno = handlerMethod.getBeanType().getAnnotation(XBStarAuthorise.class);
				autoAnno = autoAnno == null ? handlerMethod.getMethodAnnotation(XBStarAuthorise.class) : autoAnno;
				if (autoAnno == null) return true;//没有注解声明不用验证
				return onAuthoriseRequest(request);
			}
		}).addPathPatterns("/**");
	}

	@Override //改写默认的异常处理器来改写给前端的返回信息
	public void extendHandlerExceptionResolvers(List<HandlerExceptionResolver> resolvers)
	{
		resolvers.set(resolvers.size() - 1, new DefaultHandlerExceptionResolver()
		{
			@Override
			protected ModelAndView doResolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception exception)
			{
				ModelAndView result = super.doResolveException(request, response, handler, exception);
				if (result != null)
				{
					return result;
				}
				if (exception instanceof XBStarException)
				{
					// 特殊处理我们在方法中抛出的异常
					XBStarException xbstarException = (XBStarException) exception;
					makeHttpResponseErrorMessage(response, xbstarException.errorCode, xbstarException.errorMessage);
				}
				else
				{
					exception.printStackTrace();
					makeHttpResponseErrorMessage(response, 500, exception.getMessage());
				}
				return new ModelAndView();
			}

			@Override //处理@RequestParam参数没有传为null的错误
			protected ModelAndView handleMissingServletRequestParameter(MissingServletRequestParameterException ex, HttpServletRequest request, HttpServletResponse response, Object handler) throws IOException
			{
				makeHttpResponseErrorMessage(response, 400, ex.getParameterName() + "不能为空");
				return new ModelAndView();
			}

			@Override //处理@RequestParam参数不能按照配置的Convert正确转化的异常
			protected ModelAndView handleTypeMismatch(TypeMismatchException ex, HttpServletRequest request, HttpServletResponse response, Object handler) throws IOException
			{
				makeHttpResponseErrorMessage(response, 400, "参数不符合格式要求，不能不能将" + ex.getValue() + "正确转化为" + ex.getRequiredType().getName());
				return new ModelAndView();
			}

			private void makeHttpResponseErrorMessage(HttpServletResponse response, int code, String message)
			{
				response.setStatus(code);
				response.setContentType("text/plain;charset=UTF-8");
				try
				{
					response.getWriter().write(message);
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		});
	}

	@Override //自定义RequestMappingHandlerAdapter实现请求参数的解析拦截
	public RequestMappingHandlerAdapter getRequestMappingHandlerAdapter()
	{
		return new RequestMappingHandlerAdapter()
		{
			@Override
			public void afterPropertiesSet()
			{
				//afterPropertiesSet会调用getDefaultArgumentResolvers获得Spring默认的resolver，私有的不能直接掉
				super.afterPropertiesSet();
				//afterPropertiesSet之后通过getArgumentResolvers这个接口就可以获得所有的resolver
				List<HandlerMethodArgumentResolver> list = new ArrayList<>(this.getArgumentResolvers());
				// 将处理我们请求参数的一些resolver进行不改变其逻辑的继承复写，增加打印的功能
				list.set(0, new RequestParamMethodArgumentResolver(this.getBeanFactory(), false)
				{
					@Override
					protected Object resolveName(String name, MethodParameter parameter, NativeWebRequest request) throws Exception
					{
						Object result = super.resolveName(name, parameter, request);
						printRequestParameterResolveProcess(result, "RequestParamMethodArgumentResolverF", parameter);
						return result;
					}
				});
				list.set(list.size() - 2, new RequestParamMethodArgumentResolver(this.getBeanFactory(), true)
				{
					@Override
					protected Object resolveName(String name, MethodParameter parameter, NativeWebRequest request) throws Exception
					{
						Object result = super.resolveName(name, parameter, request);
						printRequestParameterResolveProcess(result, "RequestParamMethodArgumentResolverT", parameter);
						return result;
					}
				});
				this.setArgumentResolvers(list);
			}

			// 打印SpringMVC的HandlerMethodArgumentResolver参数装填信息
			public void printRequestParameterResolveProcess(Object requestValue, String resolverName, MethodParameter parameter) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
			{
				String print = ContextLoader.getCurrentWebApplicationContext().getEnvironment().getProperty("logging.xbstar");
				if (!"true".equals(print)) return;
				//初始化一些变量
				String methodParamName = parameter.getParameter().getName();
				String methodParamType = parameter.getParameterType().getSimpleName();
				String requestParamType = requestValue == null ? "NULL" : requestValue.getClass().getSimpleName();
				String converterName = "NULL_OP";
				Object converterValue = null;
				RuntimeException convertException = null;
				// 通过反射获得Converter
				TypeDescriptor sourceType = TypeDescriptor.forObject(requestValue);
				TypeDescriptor targetType = new TypeDescriptor(parameter);
				if (sourceType != null && targetType != null)
				{
					ConfigurableWebBindingInitializer initializer = (ConfigurableWebBindingInitializer) this.getWebBindingInitializer();
					GenericConversionService service = (GenericConversionService) initializer.getConversionService();
					Method getConverter = GenericConversionService.class.getDeclaredMethod("getConverter", TypeDescriptor.class, TypeDescriptor.class);
					getConverter.setAccessible(true);
					GenericConverter converter = (GenericConverter) getConverter.invoke(service, sourceType, targetType);
					converterName = converter.toString().replaceAll(" ", "");
					try
					{
						converterValue = converter.convert(requestValue, sourceType, targetType);
					} catch (Exception e)
					{
						// 就算这里不抛出convertException，原生的HandlerResolver也会抛出TypeMismatchException
						PropertyChangeEvent event = new PropertyChangeEvent(e.getMessage(), methodParamName, null, requestValue);
						convertException = new TypeMismatchException(event, parameter.getParameterType());
					}
					// 对于数据类型的数据，特殊打印一下子
					if (converterValue instanceof Object[])
					{
						String printValue = "[";
						for (Object cur : (Object[]) converterValue)
						{
							printValue += cur.toString();
							printValue += ",";
						}
						printValue += "]";
						converterValue = printValue.replace(",]", "]");
					}
				}
				// 打印请求参数与参数的转化结果
				StringBuilder build = new StringBuilder();
				build.append(requestValue);
				build.append(" (" + requestParamType + ")");
				build.append("\t-");
				build.append(resolverName);
				build.append("(" + converterName + ")->\t");
				build.append(methodParamName);
				build.append(" (" + methodParamType + ")");
				build.append("\t=\t");
				build.append(convertException == null ? converterValue : "转化出错！");
				System.out.println("\033[1;33m" + build.toString() + "\033[0m");
			}
		};
	}

	@Override //自定义Controller接收参数的转换器
	public void addFormatters(FormatterRegistry registry)
	{
		registry.addConverter(new Converter<String, LocalDate>()
		{
			@Override
			public LocalDate convert(String source)
			{
				return LocalDate.parse(source);
			}
		});
		registry.addConverter(new Converter<String, LocalDateTime>()
		{
			@Override
			public LocalDateTime convert(String source)
			{
				return LocalDateTime.parse(source, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
			}
		});
	}

	@Override //自定义Controller返回结果的处理器
	public void extendMessageConverters(List<HttpMessageConverter<?>> converters)
	{
		// 去除重复扫描到的Convert
		Set<Class<?>> set = new HashSet<>();
		new ArrayList<>(converters).forEach(converter ->
		{
			if (set.contains(converter.getClass())) converters.remove(converter);
			else set.add(converter.getClass());
		});
	}

	@Override //配置结果处理器的命中策略，优先匹配返回JSON的处理器
	public void configureContentNegotiation(ContentNegotiationConfigurer configurer)
	{
		configurer.favorPathExtension(false) //不看 .xml / .json 后缀
				.favorParameter(false) //不看 ?format=json
				.ignoreAcceptHeader(true) //看 Request请求的Accept 头
				.defaultContentType(MediaType.APPLICATION_JSON); //完全按照Convert顺序来且优先命中JSON
	}
}
