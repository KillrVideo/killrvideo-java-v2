package com.killrvideo.grpc.test.integration.step;

import static com.killrvideo.grpc.utils.GrpcMapper.uuidToUuid;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import killrvideo.user_management.UserManagementServiceGrpc.UserManagementServiceBlockingStub;
import killrvideo.user_management.UserManagementServiceOuterClass.CreateUserRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.CreateUserResponse;
import killrvideo.user_management.UserManagementServiceOuterClass.GetUserProfileRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.GetUserProfileResponse;
import killrvideo.user_management.UserManagementServiceOuterClass.UserProfile;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsRequest;
import killrvideo.user_management.UserManagementServiceOuterClass.VerifyCredentialsResponse;

public class UserManagementServiceSteps extends AbstractSteps {
    
    private static final Map<String, UserProfile> PROFILES = new HashMap<>();
    private static final Map<String, String> ERRORS = new ConcurrentHashMap<>();
    
    @Before("@user_scenarios")
    public void init() {
        truncateAllTablesFromKillrVideoKeyspace();
    }

    @After("@user_scenarios")
    public void cleanup() {
        truncateAllTablesFromKillrVideoKeyspace();
    }

    @Given("those users already exist: (.*)")
    public void createUserWithId(List<String> users) throws Exception {
        for (String user : users) {
            assertThat(testDatasetUsers)
                    .as("%s is unknown, please specify userXXX where XXX is a digit")
                    .containsKey(user);

            final CreateUserRequest request = CreateUserRequest.newBuilder()
                    .setUserId(uuidToUuid(testDatasetUsers.get(user)))
                    .setEmail(RandomStringUtils.randomAlphabetic(10) + "@gmail.com")
                    .setFirstName(RandomStringUtils.randomAlphabetic(5))
                    .setLastName(RandomStringUtils.randomAlphabetic(5))
                    .setPassword(RandomStringUtils.randomAlphabetic(10))
                    .build();

            final CreateUserResponse response = grpcClient.getUserService().createUser(request);

            assertThat(response).as("Cannot create %s", user).isNotNull();
        }

    }

    @Given("^user with email (.+) does not exist$")
    public void ensureUserDoesNotExist(String email) {
        final BoundStatement bs = findUserByEmailPs.bind(email);
        final Row foundUserByEmail = dseSession.execute(bs).one();
        assertThat(foundUserByEmail)
                .as("User with email %s should not already exist", email)
                .isNull();
    }

    @When("I create (\\d) users with email (.+) and password (.+)")
    public void createUserWithEmail(int userCount, String email, String password) throws Exception {
        List<CreateUserRequest> requests = new ArrayList<>();
        for (int i=1; i<= userCount; i++) {
            requests.add(CreateUserRequest.newBuilder()
                    .setUserId(uuidToUuid(UUID.randomUUID()))
                    .setEmail(email)
                    .setFirstName(RandomStringUtils.randomAlphabetic(5))
                    .setLastName(RandomStringUtils.randomAlphabetic(5))
                    .setPassword(password)
                    .build());
        }
        final CountDownLatch startLatch = new CountDownLatch(userCount);
        final ThreadPoolExecutor myThreadPool = new ThreadPoolExecutor(10, 10, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        requests.forEach(x -> myThreadPool.submit(createThreadForUserCreation(startLatch, grpcClient.getUserService(), x)));
        startLatch.await();
    }

    @Given("user with credentials ([^/]+)/(.+) already exists")
    public void ensureUserAlreadyExists(String email, String password) {
        final CreateUserRequest userRequest = CreateUserRequest.newBuilder()
                .setUserId(uuidToUuid(UUID.randomUUID()))
                .setEmail(email)
                .setFirstName(RandomStringUtils.randomAlphabetic(5))
                .setLastName(RandomStringUtils.randomAlphabetic(5))
                .setPassword(password)
                .build();
        final CreateUserResponse createUserResponse = grpcClient.getUserService().createUser(userRequest);
        assertThat(createUserResponse)
                .as("User with email %s and password %s has been created", email, password)
                .isNotNull();
    }

    @Then("I should be able to login with ([^/]+)/(.+)")
    public void loginWithCredentials(String email, String password) {
        final VerifyCredentialsRequest verifyCredentialsRequest = VerifyCredentialsRequest.newBuilder()
                .setEmail(email)
                .setPassword(password)
                .build();

        final VerifyCredentialsResponse verifyCredentialsResponse = grpcClient.getUserService().verifyCredentials(verifyCredentialsRequest);

        assertThat(verifyCredentialsResponse.hasUserId())
                .as("Login with email %s and password %s is successful", email, password)
                .isTrue();
    }

    @Then("I receive the '(.+)' error message for (.+) account")
    public void checkErrorsForAccount(String errorMessage, String email) {
        System.out.println(ERRORS);
        assertThat(ERRORS)
                .as("Cannot find error message %s for %s account", errorMessage, email)
                .containsKey(email);
        assertThat(ERRORS.get(email))
                .as("Cannot find error message %s for %s account", errorMessage, email)
                .contains(errorMessage);

    }

    @When("I get profile of (.+)")
    public void getProfile(String email) {
        final BoundStatement bs    = findUserByEmailPs.bind(email);
        final Row foundUserByEmail = dseSession.execute(bs).one();

        assertThat(foundUserByEmail).as("Cannot find user with email %s", email).isNotNull();

        final UUID userid = foundUserByEmail.getUUID("userid");

        assertThat(userid).as("User with email %s does not have a non-null userid", email).isNotNull();

        GetUserProfileRequest request = GetUserProfileRequest
                .newBuilder()
                .addUserIds(uuidToUuid(userid))
                .build();

        final GetUserProfileResponse response = grpcClient.getUserService().getUserProfile(request);

        assertThat(response).as("Cannot find user with email %s", email).isNotNull();
        assertThat(response.getProfilesList()).as("Cannot find user with email %s", email).hasSize(1);

        PROFILES.put(email, response.getProfiles(0));
    }

    @Then("the profile (.+) exists")
    public void ensureProfileDoesExist(String email) {
        assertThat(PROFILES)
                .as("Cannot find profile %s", email)
                .containsKey(email);

        final UserProfile userProfile = PROFILES.get(email);
        assertThat(userProfile.getEmail())
                .as("Cannot find profile %s", email)
                .isEqualTo(email);
    }

    public Runnable createThreadForUserCreation(
            final CountDownLatch startLatch,
            final UserManagementServiceBlockingStub stub,
            final CreateUserRequest request) {
        return () -> {
            try {
                stub.createUser(request);
            } catch(Exception ex) {
                ERRORS.putIfAbsent(request.getEmail(), ex.getMessage());
            } finally {
                startLatch.countDown();
            }
        };
    }
}
