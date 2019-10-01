package ltb;

import ltb.mvar_stub.MvarStub;
import org.apache.commons.codec.binary.Base64;

import java.util.Random;
import java.util.stream.Collectors;

public class MVarStub {
    private static final Random random = new Random(31416);
    String varName = "";
    private String name = "";
    private int devId = 0;
    private long index = 0;
    private String oid = "";
    private int dataType = 0;
    private int dataSourceClass = 0;
    private double sensorScale = 1.0;
    private String tags = "";
    private boolean pollingVar = false;
    private boolean string = false;

    public String serialize() {
        final MvarStub.MVarStubProto.Builder builder = MvarStub.MVarStubProto.newBuilder();
        builder
                .setVarName(varName)
                .setCompName(name)
                .setDevId(devId)
                .setIndex(index);
        if (!oid.isEmpty()) builder.setOid(oid);
        builder.setDataTypeInt(dataType);
        builder.setDataSourceClassInt(dataSourceClass);
        if (sensorScale != 1.0) builder.setSensorScale(sensorScale);
        if (tags != null && !tags.isEmpty()) builder.setTags(tags);
        builder.setPollingVar(pollingVar);
        builder.setStringType(string);

        return Base64.encodeBase64String(builder.build().toByteArray());
    }

    String triplet() {
        return String.format("%s.%d.%d", varName, devId, index);
    }

    static MVarStub newRandom(final int nVars, final int nComps, final int nDevs) {
        MVarStub m = new MVarStub();
        m.varName = String.format("variableName%d", random.nextInt(nVars));
        m.name = String.format("component%d", random.nextInt(nComps));
        m.devId = random.nextInt(nDevs) + 1;
        m.index = random.nextInt(16);
        m.oid = random.ints(random.nextInt(4)+5, 0, 32).mapToObj(String::valueOf).collect(Collectors.joining());
        m.dataType = random.nextInt(5);
        m.dataSourceClass = random.nextInt(3);
        // tags? how big are they in the typical gap case
        return m;
    }
}


