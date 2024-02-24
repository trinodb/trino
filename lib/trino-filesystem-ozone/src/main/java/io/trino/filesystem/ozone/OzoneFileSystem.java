/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.ozone;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;

public class OzoneFileSystem
        implements TrinoFileSystem
{
    private final ObjectStore objectStore;

    public OzoneFileSystem(ObjectStore objectStore)
    {
        this.objectStore = objectStore;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        return new OzoneInputFile(ozoneLocation, objectStore, OptionalLong.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        return new OzoneInputFile(ozoneLocation, objectStore, OptionalLong.of(length));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        return new OzoneOutputFile(ozoneLocation, objectStore);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        // is this needed?
        // exist in azure and s3, but not gcs
         location.verifyValidFileLocation();
        // also, blob storage should allow tailing '/' in filename? or is it not supported?

        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        try {
            bucket.deleteKey(ozoneLocation.key());
        }
        catch (OMException e) {
            if (e.getResult().equals(KEY_NOT_FOUND)) {
                return;
            }
            throw e;
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());

        String key = ozoneLocation.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        Iterator<? extends OzoneKey> iterator = bucket.listKeys(key);
        List<String> keyList = new ArrayList<>();
        iterator.forEachRemaining(f -> keyList.add(f.getName()));
        bucket.deleteKeys(keyList);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        // blob storage doesn't need to implement this
        throw new IOException("Ozone does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());
        Iterator<? extends OzoneKey> iterator = bucket.listKeys(ozoneLocation.key());
        while (iterator.hasNext()) {
            System.out.println(iterator.next().getName());
        }
        // why?
        String key = ozoneLocation.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }
        // is this ozone bug?
        if (key.equals("/")) {
            return FileIterator.empty();
        }
        return new OzoneFileIterator(ozoneLocation, bucket.listKeys(key));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        if (ozoneLocation.key().isEmpty()) {
            return Optional.of(true);
        }
        if (listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        // Ozone does not have directories
        validateOzoneLocation(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        // Ozone with File System Optimization should support this op, with the cost of some performance penalty
        // Object storage style access should not implement this
        // See: https://ozone.apache.org/docs/current/feature/prefixfso.html
        throw new IOException("Ozone does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        // TODO: is this needed for blob storage?
        // TOOD: handle volume and bucket listing
        // see org.apache.hadoop.fs.ozone.BasicRootedOzoneClientAdapterImpl#listStatus
        OzoneLocation ozoneLocation = new OzoneLocation(location);
        Location baseLocation = ozoneLocation.baseLocation();
        OzoneVolume ozoneVolume = objectStore.getVolume(ozoneLocation.volume());
        OzoneBucket bucket = ozoneVolume.getBucket(ozoneLocation.bucket());

        // TODO: copy from S3, why is this correct? what's the meaning for append "/" used with shadow=true
        // Spec says: For blob file systems, all directories containing blobs that start with the location are listed.
        String locationKey = ozoneLocation.key();
        if (!locationKey.isEmpty() && !locationKey.endsWith("/")) {
            locationKey += "/";
        }
        // This cause NullPointerException in OzoneManager (server side)
        // 2024-02-24 12:07:19 WARN  Server:3107 - IPC Server handler 74 on default port 9862, call Call#990 Retry#8 org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol.submitRequest from 172.18.0.1:48850 / 172.18.0.1:48850
        //java.lang.NullPointerException
        //	at org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.isFile(OzoneFSUtils.java:92)
        //	at org.apache.hadoop.ozone.om.KeyManagerImpl.findKeyInDbWithIterator(KeyManagerImpl.java:1707)
        //	at org.apache.hadoop.ozone.om.KeyManagerImpl.listStatus(KeyManagerImpl.java:1607)
        //	at org.apache.hadoop.ozone.om.OmMetadataReader.listStatus(OmMetadataReader.java:240)
        //	at org.apache.hadoop.ozone.om.OzoneManager.listStatus(OzoneManager.java:3672)
        //	at org.apache.hadoop.ozone.om.OzoneManager.listStatusLight(OzoneManager.java:3682)
        //	at org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler.listStatusLight(OzoneManagerRequestHandler.java:1243)
        //	at org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler.handleReadRequest(OzoneManagerRequestHandler.java:282)
        //	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.submitReadRequestToOM(OzoneManagerProtocolServerSideTranslatorPB.java:265)
        //	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.internalProcessRequest(OzoneManagerProtocolServerSideTranslatorPB.java:211)
        //	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.processRequest(OzoneManagerProtocolServerSideTranslatorPB.java:171)
        //	at org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher.processRequest(OzoneProtocolMessageDispatcher.java:89)
        //	at org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB.submitRequest(OzoneManagerProtocolServerSideTranslatorPB.java:162)
        //	at org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos$OzoneManagerService$2.callBlockingMethod(OzoneManagerProtocolProtos.java)
        //	at org.apache.hadoop.ipc.ProtobufRpcEngine$Server.processCall(ProtobufRpcEngine.java:484)
        //	at org.apache.hadoop.ipc.ProtobufRpcEngine2$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine2.java:595)
        //	at org.apache.hadoop.ipc.ProtobufRpcEngine2$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine2.java:573)
        //	at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:1227)
        //	at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:1094)
        //	at org.apache.hadoop.ipc.Server$RpcCall.run(Server.java:1017)
        //	at java.base/java.security.AccessController.doPrivileged(Native Method)
        //	at java.base/javax.security.auth.Subject.doAs(Subject.java:423)
        //	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1899)
        //	at org.apache.hadoop.ipc.Server$Handler.run(Server.java:3048)
        if (locationKey.equals("/")) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Location> builder = ImmutableSet.builder();
        Iterator<? extends OzoneKey> iteratorNotShadow = bucket.listKeys(locationKey, null, false);
        Iterator<? extends OzoneKey> iteratorShadow = bucket.listKeys(locationKey, null, true);
        while (iteratorShadow.hasNext()) {
            OzoneKey key = iteratorShadow.next();
            if (!key.isFile()) {
                String name = key.getName();
                builder.add(baseLocation.appendPath(key.getName()));
            }
        }

        return builder.build();
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        validateOzoneLocation(targetPath);
        // Ozone does not have directories
        return Optional.empty();
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateOzoneLocation(Location location)
    {
        new OzoneLocation(location);
    }
}
