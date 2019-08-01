package com.killrvideo.utils;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.google.protobuf.Timestamp;

import killrvideo.common.CommonTypes.TimeUuid;
import killrvideo.common.CommonTypes.Uuid;

/**
 * Mapping.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
public class GrpcMappingUtils {
    
    /** Hiding private constructor. */
    private GrpcMappingUtils() {}
    
    /**
     * Conversions.
     */
    
    public static TimeUuid uuidToTimeUuid(UUID uuid) {
        return TimeUuid.newBuilder().setValue(uuid.toString()).build();
    }
    
    public static Uuid uuidToUuid(UUID uuid) {
        return Uuid.newBuilder().setValue(uuid.toString()).build();
    }
    
    public static Instant timestampToInstant(Timestamp protoTimeStamp) { 
       return Instant.ofEpochSecond(
               protoTimeStamp.getSeconds(), 
               protoTimeStamp.getNanos() ) ;
    }
    
    public static Date timestampToDate(Timestamp protoTimestamp) {
        return Date.from(timestampToInstant(protoTimestamp));
    }
    
    public static Timestamp dateToTimestamp(Date date) {
        return instantToTimeStamp(date.toInstant());
    }
    
    public static Timestamp instantToTimeStamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

}
