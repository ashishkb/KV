#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include </usr/include/vsl_dp_experimental/kv.h>
#define MAX_DEV_NAME_SIZE 256

int main(int argc, char **argv)
{
    char device_name[MAX_DEV_NAME_SIZE];
    int fd = 0;
    int max_pools = 1024; /* Valid values are 1 - 1 Million */
    int pool_id = 0;
    int sector_size = 512;
    unsigned int version = 0;
    int kv_id = 0;
    bool cond_write = false;
    char *key_str = "abc_test";
    char *key = NULL;
    char *value_str = NULL;
    int ret = 0;
    int value_len = 0;
    kv_key_info_t key_info;
    int it_id = -1;
    int next_element;
    uint32_t key_len = 0;
    kv_pool_info_t pool_info;
    int optind = 0;

    if (argc < 2)
    {
        printf("usage:\n");
        printf("./kv_sample <device_name>\n");
        return -1;
    }

    /* Opening the underneath device. device_name must be /dev/fctX where X can be 0 - n */
    strncpy(device_name, argv[++optind], MAX_DEV_NAME_SIZE);
	if (strncmp(device_name, "/dev/fct", strlen("/dev/fct")) != 0)
	{
	    printf("***** Incorrect device name %s *****\n", device_name);
	    return -1;
	}

    fd = open(device_name, O_RDWR);
    if (fd < 0)
    {
        printf("could not open file %s errno %d\n", device_name, errno);
        return -1;
    }
    printf("Device id = %d\n", fd);

    /* Creating KV Store */
    kv_id = kv_create(fd, version, max_pools, cond_write);
    if (kv_id < 0)
    {
        printf("could not create kv store errno %d\n", errno);
        goto test_exit;
    }
    printf("successfully created kvstore, kv_id = %d\n", kv_id);

    pool_id = kv_pool_create(kv_id);
    if (pool_id < 0)
    {
        printf("could not create pool errno %d\n", errno);
        goto test_exit;
    }
    printf("successfully created pool with pool_id = %d\n", pool_id);

    /* Currently kv_put accepts sector aligned buffer */
    /* Allocating sector aligned buffer of size 512B; value size could be anything greater than 0 and less than 1MiB - 1KiB (1MiB less 1KiB) */
    value_len = sector_size;
    posix_memalign((void**) &value_str, sector_size,
                    value_len);
    memset(value_str, 'a', value_len);

   /* Valid values for key_len is 1 bytes to 128 bytes */ 
   /* Inserts the key into pool_id */
   ret = kv_put(kv_id, pool_id, (kv_key_t *) key_str,
                strlen((char *) key_str), value_str, value_len, 0, false, 0);
    if (ret < 0)
    {
        printf("kv_put failed errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_put succeeded\n");
    memset(value_str, '0', value_len);

    ret = kv_get(kv_id, pool_id, (kv_key_t *) key_str, strlen((char *) key_str), value_str, value_len, &key_info);
    if (ret < 0)
    {
        printf("kv_get failed errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_get succeeded\n");

    ret = kv_exists(kv_id, pool_id, (kv_key_t *) key_str, strlen((char *) key_str));
    if (ret == 0)
    {
        printf("kv_exists: key doesn't exist, errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_exists: key exists\n");

    /* KV Iterator */    
    it_id = kv_begin(kv_id, pool_id);
	if (it_id < 0)
	{
	    printf("kv_begin failed, errno %d\n", errno);
    }
    printf("kv_begin succeeded\n");

    key = malloc(strlen((char *) key_str));
    while (true)
	{
        next_element = kv_next(kv_id, pool_id, it_id);
	    if (next_element != 0)
	    {
	        if (errno != -FIO_ERR_OBJECT_NOT_FOUND)
	        {
	            printf("kv_next failed %d\n", errno);
	        }
	        break;
	    }
        printf("kv_next found an element\n");

        ret = kv_get_current(kv_id, it_id, key, &key_len, value_str, value_len, &key_info);
        if (ret < 0)
        {
            printf("kv_get_current failed errno %d\n", errno);
            goto test_exit;
        }
        printf("kv_get_current succeeded\n");
    }

    /* KV Delete */
    ret = kv_delete(kv_id, pool_id, (kv_key_t *) key_str, strlen((char *) key_str));
    if (ret < 0)
    {
        printf("kv_delete: key deletion failed with errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_delete: Deleted key successfully\n");

    ret = kv_get_pool_info(kv_id, pool_id, &pool_info);
    if (ret < 0)
    {
        printf("kv_get_pool_info failed with errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_get_pool_info succeeded: pool status = %d\n", pool_info.pool_status);

    ret = kv_pool_delete(kv_id, pool_id);
    if (ret < 0)
    {
        printf("kv_pool_delete failed with errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_pool_delete succeeded\n");

    ret = kv_get_pool_info(kv_id, pool_id, &pool_info);
    if (ret < 0)
    {
        printf("kv_get_pool_info failed with errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_get_pool_info succeeded: pool status = %d\n", pool_info.pool_status);

    ret = kv_destroy(kv_id);
    if (ret < 0)
    {
        printf("kv_destroy failed with errno %d\n", errno);
        goto test_exit;
    }
    printf("kv_destroy succeeded\n");

test_exit:
    if (key)
	{
        free(key);
	}
    if (value_str)
    {
        free(value_str);
    }
    close(fd);
    exit(0);
}
