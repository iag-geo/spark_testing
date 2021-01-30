

x = 1
y = 1
z = 1

cubes = 36

rectangular_prism_count = 0

while x <= cubes:
    while y <= cubes:
        while z <= cubes:
            volume = x * y * z

            if volume == cubes:
                rectangular_prism_count += 1

                print("{} - {} long x {} wide x {} high".format(rectangular_prism_count, x, y, z))

            z += 1

        y += 1
        z = 1

    x += 1
    y = 1

print("\n{} variations".format(rectangular_prism_count))
