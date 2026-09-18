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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.maven.model.Model;
import org.apache.maven.model.Parent;
import org.apache.maven.model.Repository;
import org.apache.maven.model.building.DefaultModelBuilderFactory;
import org.apache.maven.model.building.DefaultModelBuildingRequest;
import org.apache.maven.model.building.FileModelSource;
import org.apache.maven.model.building.ModelBuilder;
import org.apache.maven.model.building.ModelBuildingException;
import org.apache.maven.model.building.ModelBuildingRequest;
import org.apache.maven.model.building.ModelSource;
import org.apache.maven.model.building.Result;
import org.apache.maven.model.resolution.ModelResolver;
import org.apache.maven.model.resolution.UnresolvableModelException;
import org.eclipse.aether.DefaultRepositoryCache;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.ArtifactType;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.artifact.DefaultArtifactType;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.Exclusion;
import org.eclipse.aether.impl.RemoteRepositoryManager;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.repository.RepositoryPolicy;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.supplier.RepositorySystemSupplier;
import org.eclipse.aether.supplier.SessionBuilderSupplier;
import org.eclipse.aether.util.filter.ScopeDependencyFilter;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.eclipse.aether.util.artifact.JavaScopes.COMPILE;
import static org.eclipse.aether.util.artifact.JavaScopes.PROVIDED;
import static org.eclipse.aether.util.artifact.JavaScopes.RUNTIME;
import static org.eclipse.aether.util.artifact.JavaScopes.SYSTEM;
import static org.eclipse.aether.util.artifact.JavaScopes.TEST;

/// Resolves plugin classpaths with Maven Resolver, using the same logic as the trino-maven-plugin
/// packager: direct test/provided/system dependencies are dropped before collection, and the
/// resolved graph is filtered to the runtime scopes. This also keeps the provided SPI out of the
/// plugin classpath, so plugins see the server's copy through the parent class loader.
///
/// Additionally, resolved artifacts that are sibling modules of the same multi-module build are
/// remapped from the installed jar to their workspace `target/classes` directory, matching
/// the behavior of the airlift resolver this class replaced. `HdfsFileSystemLoader` relies
/// on seeing a `target/classes` directory to detect development mode and locate the HDFS
/// jars in the trino-hdfs module.
final class MavenArtifactResolver
{
    public static final String USER_LOCAL_REPO = System.getProperty("user.home") + "/.m2/repository";
    public static final String MAVEN_CENTRAL_URI = "https://repo1.maven.org/maven2/";

    private final RepositorySystem repositorySystem;
    private final RemoteRepositoryManager remoteRepositoryManager;
    private final RepositorySystemSession session;
    private final List<RemoteRepository> repositories;
    private final ModelBuilder modelBuilder = new DefaultModelBuilderFactory().newInstance();
    private final Map<String, Map<String, File>> siblingModulesCache = new ConcurrentHashMap<>();

    public MavenArtifactResolver(String localRepository, List<String> remoteRepositories)
    {
        requireNonNull(localRepository, "localRepository is null");
        requireNonNull(remoteRepositories, "remoteRepositories is null");

        RepositorySystemSupplier repositorySystemSupplier = new RepositorySystemSupplier();
        this.repositorySystem = repositorySystemSupplier.get();
        this.remoteRepositoryManager = repositorySystemSupplier.getRemoteRepositoryManager();
        this.session = new SessionBuilderSupplier(repositorySystem).get()
                .withLocalRepositoryBaseDirectories(Path.of(localRepository))
                .setSystemProperties(System.getProperties())
                // without a cache the resolver re-reads and re-models every dependency pom
                // (including the whole parent and BOM chain) for each request
                .setCache(new DefaultRepositoryCache())
                // never re-check remote metadata for locally cached snapshots
                .setUpdatePolicy(RepositoryPolicy.UPDATE_POLICY_NEVER)
                .build();

        ImmutableList.Builder<RemoteRepository> repositories = ImmutableList.builder();
        int index = 0;
        for (String remoteRepository : remoteRepositories) {
            repositories.add(new RemoteRepository.Builder("repository-" + index, "default", remoteRepository).build());
            index++;
        }
        this.repositories = repositories.build();
    }

    /// Resolves the runtime classpath of the plugin project described by the pom. The first
    /// returned artifact is the project itself, backed by its `target/classes` directory.
    public List<Artifact> resolvePom(File pomFile)
    {
        Model model = buildEffectiveModel(pomFile);

        // Drop non-runtime direct dependencies before collection, exactly like
        // TrinoPluginPackager.resolveRuntimeScopeTransitively: leaving provided/system-scoped
        // direct declarations in lets conflict mediation prefer the wrong artifact before the
        // ScopeDependencyFilter runs.
        List<Dependency> dependencies = model.getDependencies().stream()
                .filter(dependency -> isRuntimeScope(dependency.getScope()))
                .map(this::toAetherDependency)
                .collect(toImmutableList());

        Artifact projectArtifact = new DefaultArtifact(
                model.getGroupId(),
                model.getArtifactId(),
                null,
                model.getPackaging(),
                model.getVersion())
                .setFile(new File(model.getBuild().getOutputDirectory()));

        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRootArtifact(projectArtifact);
        collectRequest.setRepositories(mergeRepositories(model.getRepositories()));
        collectRequest.setDependencies(dependencies);
        collectRequest.setManagedDependencies(managedDependencies(model));

        Map<String, File> siblingModules = siblingModules(pomFile, model);
        List<Artifact> artifacts = resolveDependencies(collectRequest).stream()
                .map(artifact -> toWorkspaceArtifact(artifact, siblingModules))
                .collect(toImmutableList());

        return ImmutableList.<Artifact>builder()
                .add(projectArtifact)
                .addAll(artifacts)
                .build();
    }

    /// Resolves the artifact given by coordinates (`groupId:artifactId[:extension]:version`)
    /// together with its transitive runtime dependencies.
    public List<Artifact> resolveArtifacts(String coordinates)
    {
        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRoot(new Dependency(new DefaultArtifact(coordinates), RUNTIME));
        collectRequest.setRepositories(repositories);
        return resolveDependencies(collectRequest);
    }

    private List<Artifact> resolveDependencies(CollectRequest collectRequest)
    {
        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, new ScopeDependencyFilter(SYSTEM, PROVIDED, TEST));
        try {
            return repositorySystem.resolveDependencies(session, dependencyRequest)
                    .getArtifactResults().stream()
                    .map(ArtifactResult::getArtifact)
                    .collect(toImmutableList());
        }
        catch (DependencyResolutionException e) {
            throw new RuntimeException("Failed to resolve dependencies for " + collectRequest, e);
        }
    }

    private Model buildEffectiveModel(File pomFile)
    {
        ModelBuildingRequest request = new DefaultModelBuildingRequest()
                .setPomFile(pomFile)
                .setModelResolver(new RepositoryModelResolver(new ArrayList<>(repositories)))
                .setSystemProperties(System.getProperties())
                .setProcessPlugins(false)
                .setValidationLevel(ModelBuildingRequest.VALIDATION_LEVEL_MINIMAL);
        try {
            return modelBuilder.build(request).getEffectiveModel();
        }
        catch (ModelBuildingException e) {
            throw new RuntimeException("Failed to build effective model for " + pomFile, e);
        }
    }

    private static boolean isRuntimeScope(String scope)
    {
        return !TEST.equals(scope) && !PROVIDED.equals(scope) && !SYSTEM.equals(scope);
    }

    private List<Dependency> managedDependencies(Model model)
    {
        if (model.getDependencyManagement() == null) {
            return ImmutableList.of();
        }
        return model.getDependencyManagement().getDependencies().stream()
                .map(this::toAetherDependency)
                .collect(toImmutableList());
    }

    private Dependency toAetherDependency(org.apache.maven.model.Dependency dependency)
    {
        ArtifactType type = session.getArtifactTypeRegistry().get(dependency.getType());
        if (type == null) {
            type = new DefaultArtifactType(dependency.getType());
        }

        Artifact artifact = new DefaultArtifact(
                dependency.getGroupId(),
                dependency.getArtifactId(),
                dependency.getClassifier(),
                null,
                dependency.getVersion(),
                null,
                type);

        List<Exclusion> exclusions = dependency.getExclusions().stream()
                .map(exclusion -> new Exclusion(exclusion.getGroupId(), exclusion.getArtifactId(), "*", "*"))
                .collect(toImmutableList());

        String scope = dependency.getScope();
        if (isNullOrEmpty(scope)) {
            scope = COMPILE;
        }
        return new Dependency(artifact, scope, dependency.isOptional(), exclusions);
    }

    /// Replaces the installed jar of a sibling module with its workspace output directory.
    private static Artifact toWorkspaceArtifact(Artifact artifact, Map<String, File> siblingModules)
    {
        if (!artifact.getClassifier().isEmpty()) {
            return artifact;
        }
        File classesDirectory = siblingModules.get(artifactKey(artifact));
        if (classesDirectory == null) {
            return artifact;
        }
        return artifact.setFile(classesDirectory);
    }

    /// Maps the modules declared by the parent pom of the given pom (its build siblings) to their
    /// `target/classes` directories, skipping modules that have not been built.
    private Map<String, File> siblingModules(File pomFile, Model model)
    {
        return findParentPom(pomFile, model)
                .map(parentPom -> siblingModulesCache.computeIfAbsent(parentPom.getPath(), _ -> readSiblingModules(parentPom)))
                .orElse(ImmutableMap.of());
    }

    private static Optional<File> findParentPom(File pomFile, Model model)
    {
        Parent parent = model.getParent();
        if (parent == null || isNullOrEmpty(parent.getRelativePath())) {
            return Optional.empty();
        }
        File parentPom = new File(pomFile.getAbsoluteFile().getParentFile(), parent.getRelativePath());
        if (parentPom.isDirectory()) {
            parentPom = new File(parentPom, "pom.xml");
        }
        if (!parentPom.isFile()) {
            return Optional.empty();
        }
        return Optional.of(parentPom);
    }

    private Map<String, File> readSiblingModules(File parentPom)
    {
        Model parentModel = buildRawModel(parentPom);
        File parentDirectory = parentPom.getParentFile();
        ImmutableMap.Builder<String, File> modules = ImmutableMap.builder();
        for (String moduleName : parentModel.getModules()) {
            File moduleDirectory = new File(parentDirectory, moduleName);
            File modulePom = new File(moduleDirectory, "pom.xml");
            if (!modulePom.isFile()) {
                continue;
            }
            File classesDirectory = new File(moduleDirectory, "target/classes");
            if (!classesDirectory.isDirectory()) {
                continue;
            }
            Model module = buildRawModel(modulePom);
            String groupId = module.getGroupId();
            String version = module.getVersion();
            if (module.getParent() != null) {
                if (groupId == null) {
                    groupId = module.getParent().getGroupId();
                }
                if (version == null) {
                    version = module.getParent().getVersion();
                }
            }
            if (groupId == null || version == null) {
                continue;
            }
            modules.put(groupId + ":" + module.getArtifactId() + ":" + version, classesDirectory);
        }
        return modules.buildKeepingLast();
    }

    private Model buildRawModel(File pomFile)
    {
        Result<? extends Model> result = modelBuilder.buildRawModel(pomFile, ModelBuildingRequest.VALIDATION_LEVEL_MINIMAL, false);
        if (result.hasErrors()) {
            throw new RuntimeException("Failed to read model from " + pomFile + ": " + result.getProblems());
        }
        return result.get();
    }

    private static String artifactKey(Artifact artifact)
    {
        return artifact.getGroupId() + ":" + artifact.getArtifactId() + ":" + artifact.getVersion();
    }

    private List<RemoteRepository> mergeRepositories(List<Repository> modelRepositories)
    {
        return remoteRepositoryManager.aggregateRepositories(
                session,
                repositories,
                modelRepositories.stream()
                        .map(MavenArtifactResolver::toRemoteRepository)
                        .collect(toImmutableList()),
                true);
    }

    private static RemoteRepository toRemoteRepository(Repository repository)
    {
        return new RemoteRepository.Builder(repository.getId(), repository.getLayout(), repository.getUrl())
                .setReleasePolicy(toRepositoryPolicy(repository.getReleases()))
                .setSnapshotPolicy(toRepositoryPolicy(repository.getSnapshots()))
                .build();
    }

    private static RepositoryPolicy toRepositoryPolicy(org.apache.maven.model.RepositoryPolicy policy)
    {
        boolean enabled = true;
        String updatePolicy = RepositoryPolicy.UPDATE_POLICY_DAILY;
        String checksumPolicy = RepositoryPolicy.CHECKSUM_POLICY_WARN;
        if (policy != null) {
            enabled = policy.isEnabled();
            if (!isNullOrEmpty(policy.getUpdatePolicy())) {
                updatePolicy = policy.getUpdatePolicy();
            }
            if (!isNullOrEmpty(policy.getChecksumPolicy())) {
                checksumPolicy = policy.getChecksumPolicy();
            }
        }
        return new RepositoryPolicy(enabled, updatePolicy, checksumPolicy);
    }

    private class RepositoryModelResolver
            implements ModelResolver
    {
        private final List<RemoteRepository> modelRepositories;

        public RepositoryModelResolver(List<RemoteRepository> modelRepositories)
        {
            this.modelRepositories = requireNonNull(modelRepositories, "modelRepositories is null");
        }

        @Override
        public ModelSource resolveModel(String groupId, String artifactId, String version)
                throws UnresolvableModelException
        {
            ArtifactRequest request = new ArtifactRequest(
                    new DefaultArtifact(groupId, artifactId, "", "pom", version),
                    modelRepositories,
                    null);
            try {
                ArtifactResult result = repositorySystem.resolveArtifact(session, request);
                return new FileModelSource(result.getArtifact().getFile());
            }
            catch (ArtifactResolutionException e) {
                throw new UnresolvableModelException(e.getMessage(), groupId, artifactId, version, e);
            }
        }

        @Override
        public ModelSource resolveModel(Parent parent)
                throws UnresolvableModelException
        {
            return resolveModel(parent.getGroupId(), parent.getArtifactId(), parent.getVersion());
        }

        @Override
        public ModelSource resolveModel(org.apache.maven.model.Dependency dependency)
                throws UnresolvableModelException
        {
            return resolveModel(dependency.getGroupId(), dependency.getArtifactId(), dependency.getVersion());
        }

        @Override
        public void addRepository(Repository repository)
        {
            addRepository(repository, false);
        }

        @Override
        public void addRepository(Repository repository, boolean replace)
        {
            RemoteRepository remoteRepository = toRemoteRepository(repository);
            for (int i = 0; i < modelRepositories.size(); i++) {
                if (modelRepositories.get(i).getId().equals(remoteRepository.getId())) {
                    if (replace) {
                        modelRepositories.set(i, remoteRepository);
                    }
                    return;
                }
            }
            modelRepositories.add(remoteRepository);
        }

        @Override
        public ModelResolver newCopy()
        {
            return new RepositoryModelResolver(new ArrayList<>(modelRepositories));
        }
    }
}
