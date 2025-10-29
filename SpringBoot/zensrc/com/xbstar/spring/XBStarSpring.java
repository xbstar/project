package com.xbstar.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.TypeMismatchException;
import org.springframework.boot.SpringApplication;
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
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.support.ConfigurableWebBindingInitializer;
import org.springframework.web.context.ContextLoader;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.annotation.RequestParamMethodArgumentResolver;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.support.DefaultHandlerExceptionResolver;

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
import java.util.List;

@Configuration
@SpringBootApplication
public abstract class XBStarSpring implements WebMvcConfigurer, WebMvcRegistrations, ApplicationContextAware
{
	public boolean xbstarAuthoriseRequest(HttpServletRequest request)
	{
		return true;
	}

	public void xbstarApplicationReady() throws Exception
	{
	}

	@EventListener(ApplicationReadyEvent.class)
	private void applicationReadyEvent() throws Exception
	{
		this.xbstarApplicationReady();
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
		// 以匿名内部类的方式实现拦截器
		HandlerInterceptor interceptor = new HandlerInterceptor()
		{
			@Override
			public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception
			{
				// 所有接口都要处理response的返回头
				response.setHeader("spring-url", request.getRequestURL().toString());
				// 判断是否拦截到Rest方法
				if (!(handler instanceof HandlerMethod) || "/error".equals(request.getServletPath()))
				{
					// 拦截的不是方法或者是Spring的报错方法不用处理
					return true;
				}
				HandlerMethod handlerMethod = (HandlerMethod) handler;
				String print = ContextLoader.getCurrentWebApplicationContext().getEnvironment().getProperty("logging.xbstar");
				if ("true".equals(print)) System.out.println(request.getRequestURL()); //打印蓝色的地址出来
				// 通过反射判断是否标记了注解
				XBStarAuthorise autoAnno = handlerMethod.getBeanType().getAnnotation(XBStarAuthorise.class);
				autoAnno = autoAnno == null ? handlerMethod.getMethodAnnotation(XBStarAuthorise.class) : autoAnno;
				if (autoAnno == null) return true;//没有注解声明不用验证
				return xbstarAuthoriseRequest(request);
			}
		};
		// 向SpringMVC注入构造的拦截器
		registry.addInterceptor(interceptor).addPathPatterns("/**");
	}

	@Override //改写默认的异常处理器来改写给前端的返回信息
	public void extendHandlerExceptionResolvers(List<HandlerExceptionResolver> resolvers)
	{
		resolvers.set(resolvers.size() - 1, new DefaultHandlerExceptionResolver()
		{
			@Override
			protected ModelAndView doResolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
			{
				ModelAndView result = super.doResolveException(request, response, handler, ex);
				if (result != null)
				{
					return result;
				}
				if (ex instanceof XBStarException)
				{
					// 特殊处理我们在方法中抛出的异常
					XBStarException exception = (XBStarException) ex;
					makeHttpResponseErrorMessage(response, exception.errorCode, exception.errorMessage);
					return new ModelAndView();
				}
				return null;
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
					Method getConverter = GenericConversionService.class
							.getDeclaredMethod("getConverter", TypeDescriptor.class, TypeDescriptor.class);
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

	@Override //自定义转化器修改对日期的处理
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
}
