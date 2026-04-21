/* 
package com.cinematch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@SpringBootTest(properties = "kobis.poster-fetch.enabled=false")
class LoginApplicationTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	void contextLoads() {
	}

	@Test
	void loginPageLoads() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(get("/"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/login"));
	}

	@Test
	void signupPageLoads() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(get("/signup"))
				.andExpect(status().isOk())
				.andExpect(view().name("signup-page"));
	}

	@Test
	void registeredUserCanEnterChartsAndMovieDetail() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		mockMvc.perform(get("/charts").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("index"))
				.andExpect(model().attributeExists("movies"))
				.andExpect(model().attribute("loginUserNickname", "A137"))
				.andExpect(model().attributeExists("currentPage"))
				.andExpect(model().attributeExists("totalPages"));

		mockMvc.perform(get("/movies/20129370").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("movie-detail"))
				.andExpect(model().attributeExists("movie"))
				.andExpect(model().attributeExists("genres"))
				.andExpect(model().attributeExists("directors"))
				.andExpect(model().attributeExists("actors"))
				.andExpect(model().attributeExists("companies"))
				.andExpect(model().attributeExists("audits"));
	}

	@Test
	void userCanLikeMovieFromMovieDetailPage() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		org.springframework.mock.web.MockHttpSession session =
				(org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false);

		Integer beforeLikeRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_like uml
				JOIN "USER" u ON u.id = uml.user_id
				JOIN movie m ON m.id = uml.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		mockMvc.perform(post("/movies/20129370/like").session(session))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/movies/20129370"));

		Integer afterFirstLikeRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_like uml
				JOIN "USER" u ON u.id = uml.user_id
				JOIN movie m ON m.id = uml.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(beforeLikeRows).isZero();
		org.assertj.core.api.Assertions.assertThat(afterFirstLikeRows).isEqualTo(1);

		mockMvc.perform(post("/movies/20129370/like").session(session))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/movies/20129370"));

		Integer afterSecondLikeRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_like uml
				JOIN "USER" u ON u.id = uml.user_id
				JOIN movie m ON m.id = uml.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(afterSecondLikeRows).isZero();
	}

	@Test
	void registeredUserCanOpenMyPage() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		mockMvc.perform(get("/mypage").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("my-page"))
				.andExpect(model().attributeExists("userProfile"))
				.andExpect(model().attributeExists("lifeMovies"))
				.andExpect(model().attributeExists("storedMovies"))
				.andExpect(model().attribute("loginUserNickname", "A137"));
	}

	@Test
	void userCanSearchAndAddLifeMovieFromMyPage() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		org.springframework.mock.web.MockHttpSession session =
				(org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false);

		mockMvc.perform(get("/mypage")
						.param("showLifeSearch", "true")
						.param("lifeQuery", "명량")
						.session(session))
				.andExpect(status().isOk())
				.andExpect(view().name("my-page"))
				.andExpect(model().attributeExists("lifeSearchResults"));

		Integer beforeLifeRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_life uml
				JOIN "USER" u ON u.id = uml.user_id
				JOIN movie m ON m.id = uml.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		mockMvc.perform(post("/mypage/life")
						.param("movieCode", "20129370")
						.session(session))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/mypage?showLifeSearch=true"));

		Integer afterFirstAddRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_life uml
				JOIN "USER" u ON u.id = uml.user_id
				JOIN movie m ON m.id = uml.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		mockMvc.perform(post("/mypage/life")
						.param("movieCode", "20129370")
						.session(session))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/mypage?showLifeSearch=true"));

		Integer afterSecondAddRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_life uml
				JOIN "USER" u ON u.id = uml.user_id
				JOIN movie m ON m.id = uml.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(beforeLifeRows).isZero();
		org.assertj.core.api.Assertions.assertThat(afterFirstAddRows).isEqualTo(1);
		org.assertj.core.api.Assertions.assertThat(afterSecondAddRows).isEqualTo(1);
	}

	@Test
	void userCanStoreMovieAndBrowseStoredPage() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		org.springframework.mock.web.MockHttpSession session =
				(org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false);

		Integer beforeStoreRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_store ums
				JOIN "USER" u ON u.id = ums.user_id
				JOIN movie m ON m.id = ums.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		mockMvc.perform(post("/movies/20129370/store").session(session))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/movies/20129370"));

		Integer afterStoreRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_store ums
				JOIN "USER" u ON u.id = ums.user_id
				JOIN movie m ON m.id = ums.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(beforeStoreRows).isZero();
		org.assertj.core.api.Assertions.assertThat(afterStoreRows).isEqualTo(1);

		mockMvc.perform(get("/mypage").session(session))
				.andExpect(status().isOk())
				.andExpect(view().name("my-page"))
				.andExpect(model().attributeExists("storedMovies"))
				.andExpect(model().attribute("storedMovieCount", 1));

		mockMvc.perform(get("/stored").session(session))
				.andExpect(status().isOk())
				.andExpect(view().name("stored-page"))
				.andExpect(model().attributeExists("storedMovies"));

		mockMvc.perform(post("/movies/20129370/store").session(session))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/movies/20129370"));

		Integer afterUnstoreRows = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM user_movie_store ums
				JOIN "USER" u ON u.id = ums.user_id
				JOIN movie m ON m.id = ums.movie_id
				WHERE u.login_id = 'movieadmin'
				  AND m.movie_cd = '20129370'
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(afterUnstoreRows).isZero();
	}

	@Test
	void invalidUserStaysOnLoginPage() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(post("/login")
						.param("id", "unknown")
						.param("pw", "badpw"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/login?error=1"));
	}

	@Test
	void signInCreatesUserInUserTable() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		mockMvc.perform(post("/signup")
						.param("id", "newuser")
						.param("pw", "4321")
						.param("nickname", "N427")
						.param("gender", "FEMALE")
						.param("age", "27"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/login"));

		Integer createdCount = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM "USER"
				WHERE login_id = 'newuser'
				  AND login_pw = '4321'
				  AND nickname = 'N427'
				  AND gender = 'FEMALE'
				  AND age = 27
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(createdCount).isEqualTo(1);
	}

	@Test
	void chartUsesTenMoviesPerPageAcrossEntireMovieList() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		mockMvc.perform(get("/charts?page=2").session((org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false)))
				.andExpect(status().isOk())
				.andExpect(view().name("index"))
				.andExpect(model().attributeExists("movies"))
				.andExpect(model().attributeExists("currentPage"))
				.andExpect(model().attributeExists("totalPages"));

		Integer totalMovieCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM movie", Integer.class);
		Integer pageTwoMovieCount = jdbcTemplate.queryForObject("""
				SELECT COUNT(*)
				FROM (
					SELECT id
					FROM movie
					ORDER BY ranking ASC
					LIMIT 10 OFFSET 10
				) page_two_movies
				""", Integer.class);

		org.assertj.core.api.Assertions.assertThat(totalMovieCount).isGreaterThan(10);
		org.assertj.core.api.Assertions.assertThat(pageTwoMovieCount).isEqualTo(10);
	}

	@Test
	void chartSupportsQueryAndGenreSearch() throws Exception {
		MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new MovieController(jdbcTemplate)).build();

		MvcResult loginResult = mockMvc.perform(post("/login")
						.param("id", "movieadmin")
						.param("pw", "0000"))
				.andExpect(status().is3xxRedirection())
				.andExpect(redirectedUrl("/charts"))
				.andReturn();

		org.springframework.mock.web.MockHttpSession session =
				(org.springframework.mock.web.MockHttpSession) loginResult.getRequest().getSession(false);

		mockMvc.perform(get("/charts")
						.param("query", "명량")
						.param("showAdvanced", "true")
						.session(session))
				.andExpect(status().isOk())
				.andExpect(view().name("index"))
				.andExpect(model().attribute("query", "명량"))
				.andExpect(model().attribute("showAdvanced", true))
				.andExpect(model().attributeExists("genres"));

		mockMvc.perform(get("/charts")
						.param("genre", "사극")
						.param("showAdvanced", "true")
						.session(session))
				.andExpect(status().isOk())
				.andExpect(view().name("index"))
				.andExpect(model().attribute("selectedGenre", "사극"))
				.andExpect(model().attribute("showAdvanced", true));
	}

}

*/