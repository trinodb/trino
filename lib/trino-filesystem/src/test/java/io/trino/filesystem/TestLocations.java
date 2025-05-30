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
package io.trino.filesystem;

import org.junit.jupiter.api.Test;

import static io.trino.filesystem.Locations.appendPath;
import static io.trino.filesystem.Locations.areDirectoryLocationsEquivalent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocations
{
    @Test
    void testAppendPath()
    {
        assertThat(appendPath("/test_dir", "test_file.txt")).isEqualTo("/test_dir/test_file.txt");
        assertThat(appendPath("/test_dir/", "test_file.txt")).isEqualTo("/test_dir/test_file.txt");
        assertThat(appendPath("/test_dir", "test_dir2/")).isEqualTo("/test_dir/test_dir2/");
        assertThat(appendPath("/test_dir/", "test_dir2/")).isEqualTo("/test_dir/test_dir2/");
        assertThat(appendPath("s3://test_dir/", "test_file.txt")).isEqualTo("s3://test_dir/test_file.txt");
        assertThat(appendPath("s3://test_dir/", "location?")).isEqualTo("s3://test_dir/location?");
        assertThat(appendPath("s3://test_dir/", "location#")).isEqualTo("s3://test_dir/location#");
        assertThat(appendPath("s3://test_dir/", "test_dir2/")).isEqualTo("s3://test_dir/test_dir2/");
        assertThat(appendPath("s3://dir_with_space /", "test_file.txt")).isEqualTo("s3://dir_with_space /test_file.txt");
        assertThat(appendPath("s3://dir_with_space /", "location?")).isEqualTo("s3://dir_with_space /location?");
        assertThat(appendPath("s3://dir_with_space /", "location#")).isEqualTo("s3://dir_with_space /location#");
        assertThat(appendPath("s3://dir_with_space /", "test_dir2/")).isEqualTo("s3://dir_with_space /test_dir2/");
        assertThat(appendPath("s3://dir_with_double_space  /", "test_file.txt")).isEqualTo("s3://dir_with_double_space  /test_file.txt");
        assertThat(appendPath("s3://dir_with_double_space  /", "location?")).isEqualTo("s3://dir_with_double_space  /location?");
        assertThat(appendPath("s3://dir_with_double_space  /", "location#")).isEqualTo("s3://dir_with_double_space  /location#");
        assertThat(appendPath("s3://dir_with_double_space  /", "test_dir2/")).isEqualTo("s3://dir_with_double_space  /test_dir2/");

        assertThatThrownBy(() -> appendPath("", "test_file.txt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("location is empty");
        assertThatThrownBy(() -> appendPath("test_dir", "test_file.txt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No scheme for file system location: test_dir");
        assertThatThrownBy(() -> appendPath("s3:test_dir", "test_file.txt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path must begin with a '/' when no authority is present");
        assertThatThrownBy(() -> appendPath("s3://test_dir", "test_file.txt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: s3://test_dir");
        assertThatThrownBy(() -> appendPath("s3://test_dir ", "test_file.txt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: s3://test_dir ");
        assertThatThrownBy(() -> appendPath("s3://user@host:invalid_port", "test_file.txt"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid port in file system location: s3://user@host:invalid_port");
    }

    @Test
    public void testDirectoryLocationEquivalence()
    {
        assertDirectoryLocationEquivalence("scheme://authority/", "scheme://authority/", true);
        assertDirectoryLocationEquivalence("scheme://authority/", "scheme://authority//", false);
        assertDirectoryLocationEquivalence("scheme://authority/", "scheme://authority///", false);
        assertDirectoryLocationEquivalence("scheme://userInfo@host:1234/dir", "scheme://userInfo@host:1234/dir/", true);
        assertDirectoryLocationEquivalence("scheme://authority/some/path", "scheme://authority/some/path", true);
        assertDirectoryLocationEquivalence("scheme://authority/some/path", "scheme://authority/some/path/", true);
        assertDirectoryLocationEquivalence("scheme://authority/some/path", "scheme://authority/some/path//", false);

        assertDirectoryLocationEquivalence("scheme://authority/some/path//", "scheme://authority/some/path//", true);
        assertDirectoryLocationEquivalence("scheme://authority/some/path/", "scheme://authority/some/path//", false);
        assertDirectoryLocationEquivalence("scheme://authority/some/path//", "scheme://authority/some/path///", false);

        assertDirectoryLocationEquivalence("scheme://authority/some//path", "scheme://authority/some//path/", true);
    }

    private static void assertDirectoryLocationEquivalence(String leftLocation, String rightLocation, boolean equivalent)
    {
        assertThat(areDirectoryLocationsEquivalent(Location.of(leftLocation), Location.of(rightLocation))).as("equivalence of '%s' in relation to '%s'", leftLocation, rightLocation)
                .isEqualTo(equivalent);
        assertThat(areDirectoryLocationsEquivalent(Location.of(rightLocation), Location.of(leftLocation))).as("equivalence of '%s' in relation to '%s'", rightLocation, leftLocation)
                .isEqualTo(equivalent);
    }

    @Test
    void testIsS3Tables()
    {
        assertThat(Locations.isS3Tables("s3://e97725d9-dbfb-4334-784sox7edps35ncq16arh546frqa1use2b--table-s3")).isTrue();
        assertThat(Locations.isS3Tables("s3://75fed916-b871-4909-mx9t6iohbseks57q16e5y6nf1c8gguse2b--table-s3")).isTrue();

        assertThat(Locations.isS3Tables("s3://e97725d9-dbfb-4334-784sox7edps35ncq16arh546frqa1use2b--table-s3/")).isFalse();
        assertThat(Locations.isS3Tables("s3://75fed916-b871-4909-mx9t6iohbseks57q16e5y6nf1c8gguse2b--table-s3/")).isFalse();
        assertThat(Locations.isS3Tables("s3://75fed916-b871-4909/mx9t6iohbseks57q16e5y6nf1c8gguse2b--table-s3")).isFalse();
        assertThat(Locations.isS3Tables("s3://test-bucket")).isFalse();
        assertThat(Locations.isS3Tables("s3://test-bucket/default")).isFalse();
    }
}
