package com.netflix.imflibrary.st0377.header;

import com.netflix.imflibrary.IMFErrorLogger;
import com.netflix.imflibrary.KLVPacket;
import com.netflix.imflibrary.MXFUID;
import com.netflix.imflibrary.annotations.MXFProperty;
import com.netflix.imflibrary.st0377.CompoundDataTypes;
import com.netflix.imflibrary.utils.ByteProvider;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Immutable
public class MultipleDescriptor extends FileDescriptor {
    private static final String ERROR_DESCRIPTION_PREFIX = "MXF Header Partition: " + MultipleDescriptor.class.getSimpleName() + " : ";
    private final MultipleDescriptorB0 multipleDescriptorB0;

    public MultipleDescriptor(MultipleDescriptorB0 multipleDescriptorB0) {
        this.multipleDescriptorB0 = multipleDescriptorB0;
    }

    @Immutable
    @SuppressWarnings({"PMD.FinalFieldCouldBeStatic"})
    public static final class MultipleDescriptorB0 extends FileDescriptor.FileDescriptorBO {
        @MXFProperty(size=0, depends=true)
        private final CompoundDataTypes.MXFCollections.MXFCollection<StrongRef> subdescriptor_uids = null;
        private final List<MXFUID> subDescriptors = new ArrayList<>();

        public MultipleDescriptorB0(KLVPacket.Header header, ByteProvider byteProvider, Map<Integer, MXFUID> localTagToUIDMap, IMFErrorLogger logger)
            throws IOException {
            super(header);
            long numBytesToRead = this.header.getVSize();
            StructuralMetadata.populate(this, byteProvider, numBytesToRead, localTagToUIDMap);
            if (this.instance_uid == null) {
                logger.addError(IMFErrorLogger.IMFErrors.ErrorCodes.IMF_ESSENCE_METADATA_ERROR, IMFErrorLogger.IMFErrors.ErrorLevels.NON_FATAL,
                        MultipleDescriptor.ERROR_DESCRIPTION_PREFIX + "instance_uid is null");
            }

            if (this.subdescriptor_uids == null) {
                logger.addError(IMFErrorLogger.IMFErrors.ErrorCodes.IMF_ESSENCE_METADATA_ERROR, IMFErrorLogger.IMFErrors.ErrorLevels.NON_FATAL,
                        MultipleDescriptor.ERROR_DESCRIPTION_PREFIX + "subdescriptor_uids is null");
            } else {
                for (StrongRef ref : subdescriptor_uids.getEntries()) {
                    this.subDescriptors.add(ref.getInstanceUID());
                }
            }
        }

        public List<MXFUID> getSubdescriptorUids() {
            return Collections.unmodifiableList(this.subDescriptors);
        }
    }
}
