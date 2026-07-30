package com.logistream.warehouse.config;

import com.logistream.common.model.PackageState;
import com.logistream.warehouse.model.PackageEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.EnableStateMachineFactory;
import org.springframework.statemachine.config.EnumStateMachineConfigurerAdapter;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;

import java.util.EnumSet;

@Configuration
@EnableStateMachineFactory
public class StateMachineConfig extends EnumStateMachineConfigurerAdapter<PackageState, PackageEvent> {

    @Override
    public void configure(StateMachineStateConfigurer<PackageState, PackageEvent> states) throws Exception {
        states
            .withStates()
            .initial(PackageState.MANIFESTED)
            .states(EnumSet.allOf(PackageState.class));
    }

    @Override
    public void configure(StateMachineTransitionConfigurer<PackageState, PackageEvent> transitions) throws Exception {
        transitions
            .withExternal()
                .source(PackageState.MANIFESTED).target(PackageState.HUB_SORTING)
                .event(PackageEvent.START_SORTING)
                .and()
            .withExternal()
                .source(PackageState.HUB_SORTING).target(PackageState.IN_TRANSIT)
                .event(PackageEvent.FINISH_SORTING)
                .and()
            .withExternal()
                .source(PackageState.IN_TRANSIT).target(PackageState.OUT_FOR_DELIVERY)
                .event(PackageEvent.OUT_FOR_DELIVERY)
                .and()
            .withExternal()
                .source(PackageState.OUT_FOR_DELIVERY).target(PackageState.DELIVERED)
                .event(PackageEvent.DELIVER);
    }
}
