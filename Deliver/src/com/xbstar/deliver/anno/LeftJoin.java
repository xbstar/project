package com.xbstar.deliver.anno;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Repeatable(LeftJoins.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface LeftJoin
{
	Class<? extends Enum> tb();

	String[] on() default {};
}
