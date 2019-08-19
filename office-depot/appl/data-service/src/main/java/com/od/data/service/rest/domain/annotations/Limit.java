package com.od.data.service.rest.domain.annotations;

import com.od.data.service.rest.domain.validators.LimitValidator;

import javax.validation.Constraint;
import javax.validation.Payload;

import java.lang.annotation.*;

/**
 * @author sandeep
 *
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = LimitValidator.class)
@Documented
public @interface Limit {
  String message() default "";

  Class<?>[] groups() default { };

  Class<? extends Payload>[] payload() default { };
}
