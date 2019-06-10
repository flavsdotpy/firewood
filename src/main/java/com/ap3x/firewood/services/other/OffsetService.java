package com.ap3x.firewood.services.other;

import com.ap3x.firewood.models.DBOffset;
import org.springframework.stereotype.Service;

@Service
public interface OffsetService {

    DBOffset getOffset(final String outputTable);
    Boolean updateOffset(final DBOffset offset);

}
