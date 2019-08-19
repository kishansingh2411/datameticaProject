package com.od.data.service.rest.domain.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;

import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.validators.FiltersValidator;

/**
 * @author sandeep
 *
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = FiltersValidator.class)
@Documented
public @interface Filters {
  String message() default "";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  Module module() default Module.CLICKSTREAM;
}
