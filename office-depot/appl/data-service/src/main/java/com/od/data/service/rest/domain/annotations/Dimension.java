package com.od.data.service.rest.domain.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;

import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.validators.DimensionValidator;

/**
 * @author sandeep
 * 
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = DimensionValidator.class)
@Documented
public @interface Dimension {
  String message() default "";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  Module module() default Module.CLICKSTREAM;

}
