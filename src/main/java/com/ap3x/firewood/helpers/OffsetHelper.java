package com.ap3x.firewood.helpers;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.services.other.OffsetService;
import com.ap3x.firewood.models.DBOffset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component(value = "offsetHelper")
public class OffsetHelper {

    private static final Log LOGGER = LogFactory.getLog(OffsetHelper.class);

    @Autowired
    private FirewoodContext faio;

    @Autowired
    private Environment env;

    public DBOffset getOffset(final String outputTable) {
        LOGGER.debug("getOffset() - Get offset by");

        return faio.getBean(
                env.getProperty("offset.engine").concat("Service"),
                OffsetService.class
        ).getOffset(outputTable);
    }

    public Boolean updateOffset(final DBOffset offset) {
        LOGGER.debug("updateOffset() - Updating offset data");

        return faio.getBean(
                env.getProperty("offset.engine").concat("Service"),
                OffsetService.class
        ).updateOffset(offset);
    }

}
