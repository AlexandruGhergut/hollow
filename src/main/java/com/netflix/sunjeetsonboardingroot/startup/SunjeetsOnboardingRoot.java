package com.netflix.sunjeetsonboardingroot.startup;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.guice.servlet.WebApplicationInitializer;

import com.netflix.sunjeetsonboardingroot.OnboardingItemsProducer;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The "main" class that boots up the service. For local testing one simply invokes the
 * main() method as if running a normal Java app.
 *
 * @author This file is auto-generated by runtime@netflix.com. Feel free to modify.
 */
@Path(JerseyModule.PRODUCE_ONCE_PATH + "{path: (/.*)?}")
public class SunjeetsOnboardingRoot implements WebApplicationInitializer {

    private static final Logger logger = LoggerFactory.getLogger(SunjeetsOnboardingRoot.class);

    private OnboardingItemsProducer onboardingItemsProducer;

    @GET
    @Produces(MediaType.TEXT_HTML)
    public Response publishOnce() throws Exception {
        logger.info("SNAP: Start publishing once");

        Injector injector = InjectorBuilder.fromModules(new SunjeetsOnboardingRootModule()).createInjector();
        this.onboardingItemsProducer = injector.getInstance(OnboardingItemsProducer.class);
        this.onboardingItemsProducer.publishData(true);

        logger.info("SNAP: Published.");

        return Response.ok().build();
    }



    public static void publishEndlessly() throws Exception {
        logger.info("SNAP: Start publishing endlessly");
        Injector injector = InjectorBuilder.fromModules(new SunjeetsOnboardingRootModule()).createInjector();
        OnboardingItemsProducer onboardingItemsProducer = injector.getInstance(OnboardingItemsProducer.class);

        while (true) {
            Thread.sleep(10*1000);
            System.out.println("SNAP: Publishing,");
            logger.info("SNAP: Publishing,");

            onboardingItemsProducer.publishData(true);

            System.out.println("SNAP: Published.");
            logger.info("SNAP: Published.");
        }
    }

    public static void main(String[] args) throws Exception {
        logger.info("SNAP: Stepped into main");
        publishEndlessly();
    }

    @Override
    public Injector createInjector() {
        return InjectorBuilder.fromModules(new SunjeetsOnboardingRootModule()).createInjector();
    }
}
