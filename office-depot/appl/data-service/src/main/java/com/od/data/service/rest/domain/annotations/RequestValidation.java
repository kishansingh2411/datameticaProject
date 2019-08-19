package com.od.data.service.rest.domain.annotations;

import com.od.data.service.rest.domain.validators.RequestValidator;

import javax.validation.Constraint;
import javax.validation.Payload;

import java.lang.annotation.*;

/**
 * @author sandeep
 *
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = RequestValidator.class)
@Documented
public @interface RequestValidation {
  String message() default "";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
