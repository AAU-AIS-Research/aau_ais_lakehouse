call ducklake_expire_snapshots('lakehouse', older_than => now() - INTERVAL '1 hour');
call ducklake_merge_adjacent_files('lakehouse');
call ducklake_cleanup_old_files('lakehouse', cleanup_all => true);